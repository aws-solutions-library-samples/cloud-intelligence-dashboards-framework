""" Dataset-level taxonomy merging

Lets the user combine several taxonomy dimensions into a single QuickSight
calculated field at dataset patch time. Sources can be:
 - tag fields extracted from tags_json (resource tags, account tags,
   IAM principal tags, user attributes, cost categories) - the same fields
   that are already injected as parseJson(...) custom fields
 - account_map columns (business unit, cost center or any other attribute
   the customer has in their account metadata)

No Athena views are modified: the merge is a QuickSight calculated field
COALESCE over the per-dimension sources, added via the same custom_fields
mechanism used for tag fields.

The merge definition is persisted in the 'taxonomy-merges' parameter using
the format: merged_name=field1+field2+field3,other_name=field4+field5
Field order defines the COALESCE priority (first non-empty wins).
"""
import re
import logging

from cid.utils import (
    all_yes,
    cid_print,
    get_defaults,
    get_parameter,
    get_parameters,
    get_yesno_parameter,
    isatty,
    set_parameters,
    unset_parameter,
)

logger = logging.getLogger(__name__)

PARAM_NAME = 'taxonomy-merges'

# display labels for the origin of a field, by field name prefix (most specific first)
SOURCE_LABELS = [
    ('iam_principal_tag_', 'IAM principal tags'),
    ('user_attribute_tag_', 'user attributes'),
    ('account_tag_', 'account tags'),
    ('cost_category_', 'cost categories'),
    ('tag_', 'resource tags'),
]


# tokens that come from our own prefixes/suffixes, not from the customer's tag names
PREFIX_TOKENS = frozenset(['tag', 'iam', 'principal', 'account', 'user', 'attribute', 'cost', 'category', 'merged'])


def sanitize_name(name: str) -> str:
    """ Sanitize a merged column name the same way as tag display names
    (QS parseJson and calculated fields do not like special characters)
    """
    return re.sub(r'\W', '_', name.strip())


def field_tokens(field: str) -> list:
    """ Meaningful lowercase tokens of a field name (our prefixes stripped) """
    for prefix, _ in SOURCE_LABELS:
        if field.startswith(prefix):
            field = field[len(prefix):]
            break
    return [token for token in re.split(r'[_\W]+', field.lower()) if token and token not in PREFIX_TOKENS]


def parse_merges(value) -> dict:
    """ Parse the taxonomy-merges parameter value.

    :param value: 'name=field1+field2,name2=field3+field4' or a list of 'name=field1+field2'
    :returns: {merged_name: [field, ...]} preserving field order
    """
    if isinstance(value, str):
        value = [group for group in value.split(',') if group.strip()]
    merges = {}
    for group in value or []:
        if '=' not in group:
            logger.warning(f'Invalid taxonomy merge group {group!r}. Expected format name=field1+field2. Skipping.')
            continue
        name, _, fields = group.partition('=')
        fields = [field.strip() for field in fields.split('+') if field.strip()]
        name = sanitize_name(name)
        if not name or not fields:
            logger.warning(f'Invalid taxonomy merge group {group!r}. Skipping.')
            continue
        merges[name] = fields
    return merges


def serialize_merges(merges: dict) -> str:
    """ Serialize merges back to the parameter format """
    return ','.join(f"{name}={'+'.join(fields)}" for name, fields in merges.items())


class TaxonomyMerger:
    """ Resolves user-selected taxonomy merges and builds QuickSight calculated fields

    :param tag_fields: names of the tag custom fields injected in this dataset
        (keys of the parseJson custom_fields dict)
    :param account_map_columns: columns of the account_map table joined in this dataset
    """

    def __init__(self, tag_fields: list, account_map_columns: list=None):
        self.tag_fields = list(tag_fields or [])
        self.account_map_columns = list(account_map_columns or [])

    @property
    def available_fields(self) -> list:
        """ All fields that can participate in a merge, tag fields first """
        return self.tag_fields + [col for col in self.account_map_columns if col not in self.tag_fields]

    def source_label(self, field: str) -> str:
        """ Human readable origin of a field: resource tag, IAM principal tag, account mapping... """
        if field in self.tag_fields:
            for prefix, label in SOURCE_LABELS:
                if field.startswith(prefix):
                    return label
            return 'other'  # e.g. iam_principal from the line_item_iam_principal CUR column
        return 'account mapping'

    GROUP_ORDER = ['resource tags', 'IAM principal tags', 'account tags', 'user attributes', 'cost categories', 'account mapping', 'other']

    def grouped_choices(self, fields: list) -> dict:
        """ {source_label: [field, ...]} for selection prompts, so fields are
        listed under their source header (resource tag, IAM principal tag, ...) """
        groups = {}
        for field in fields:
            groups.setdefault(self.source_label(field), []).append(field)
        return {label: groups[label] for label in self.GROUP_ORDER if label in groups} | \
               {label: items for label, items in groups.items() if label not in self.GROUP_ORDER}

    def suggest_name(self, fields: list, taken=()) -> str:
        """ Suggest a name for a merged column from the common tokens of its source fields.

        ['iam_principal_tag_X_Application', 'tag_Y_application'] -> 'application_merged'
        Falls back to 'merged_<first field>' when fields share no tokens and
        appends a number if the suggestion is already taken.
        """
        common = None
        for field in fields:
            tokens = set(field_tokens(field))
            common = tokens if common is None else common & tokens
        if common:
            # preserve token order of the first field
            base = '_'.join(token for token in field_tokens(fields[0]) if token in common) + '_merged'
        else:
            base = 'merged_' + sanitize_name(fields[0])
        taken = set(taken) | set(self.available_fields)
        name = base
        index = 2
        while name in taken:
            name = f'{base}_{index}'
            index += 1
        return name

    def describe_merge(self, name: str, fields: list) -> str:
        """ One-line, plain words description of the merge priority:
        'team = IAM principal tag ..._Team, else resource tag tag_..._team'
        """
        parts = [f'{self.source_label(field)} <BOLD>{field}<END>' for field in fields]
        return f"<BOLD>{name}<END> = {', else '.join(parts)}"

    def source_expression(self, field: str) -> str:
        """ QuickSight expression for a single merge source.

        Tag fields are read from tags_json with the same expression as their
        standalone custom field. Account map fields are direct column references.
        """
        if field in self.tag_fields:
            return f"parseJson(tags_json, '$.{field}')"
        return f'{{{field}}}'

    def build_merge_expression(self, fields: list) -> str:
        """ Build a QuickSight COALESCE expression over the given fields.

        Each source is wrapped in nullIf(x, '') so that empty-string tag values
        fall through to the next source, same as NULL for missing json keys.
        """
        args = ', '.join(f"nullIf({self.source_expression(field)}, '')" for field in fields)
        if len(fields) == 1:
            return args
        return f'coalesce({args})'

    def build_custom_fields(self, merges: dict) -> dict:
        """ Build {merged_name: expression} for the fields available in this dataset.

        Groups are filtered to the fields present in the current dataset
        (a merge can reference account_map columns that only exist in some datasets).
        Groups with no available fields are skipped.
        """
        custom_fields = {}
        available = set(self.available_fields)
        for name, fields in merges.items():
            usable = [field for field in fields if field in available]
            missing = [field for field in fields if field not in available]
            if missing:
                logger.info(f'Taxonomy merge {name!r}: fields {missing} not available in this dataset. Merging {usable}.')
            if not usable:
                logger.info(f'Taxonomy merge {name!r}: no fields available in this dataset. Skipping.')
                continue
            if name in available:
                logger.warning(f'Taxonomy merge name {name!r} collides with an existing field. Skipping.')
                continue
            custom_fields[name] = self.build_merge_expression(usable)
        return custom_fields

    def display_merges(self, merges: dict) -> None:
        """ Print merge groups as: name [field1 + field2 + ...] """
        for name, fields in merges.items():
            cid_print(f"  <BOLD>{name}<END> [{' + '.join(fields)}]")

    def resolve_merges(self) -> dict:
        """ Determine merge groups.

        Priority:
          1. --taxonomy-merges command line parameter
          2. configuration stored from a previous run (Athena cid_parameters,
             loaded as defaults) - shown to the user for confirmation
          3. interactive prompts
        Non-interactive sessions use 1 or 2 or skip merging.

        :returns: {merged_name: [field, ...]}
        """
        value = get_parameters().get(PARAM_NAME)
        if value is not None:
            merges = parse_merges(value)
            logger.info(f'Using taxonomy merges from parameters: {merges}')
            return merges

        # Configuration from a previous run (persisted in Athena cid_parameters view)
        stored = get_defaults().get(PARAM_NAME)
        if stored is not None:
            merges = parse_merges(stored)
            if not isatty() or all_yes():
                logger.info(f'Using stored taxonomy merges: {merges}')
                set_parameters({PARAM_NAME: stored})
                return merges
            available = set(self.available_fields)
            all_available = all(field in available for fields in merges.values() for field in fields)
            if not all_available:
                # stored config references fields that are gone (tag deselected, column removed):
                # ignore it and reconfigure from scratch
                logger.debug(f'Stored taxonomy merges reference unavailable fields. Ignoring stored config: {merges}')
            else:
                if merges:
                    cid_print('\n<BOLD>Merged taxonomy columns from the previous run:<END>')
                    self.display_merges(merges)
                if get_yesno_parameter(
                    param_name='taxonomy-merge-keep',
                    message='Keep this taxonomy merge configuration?' if merges else 'Previously you chose not to merge taxonomy fields. Keep it that way?',
                    default='yes',
                ):
                    unset_parameter('taxonomy-merge-keep')
                    set_parameters({PARAM_NAME: stored})
                    return merges
                unset_parameter('taxonomy-merge-keep')
            # fall through to reconfigure interactively

        if not isatty():
            logger.info('Non-interactive session and no --taxonomy-merges provided. Skipping taxonomy merge.')
            return {}

        if all_yes():
            # -y answers yes to everything; do not drag the user into merge prompts
            logger.info('All-yes mode and no --taxonomy-merges provided. Skipping taxonomy merge.')
            return {}

        if len(self.available_fields) < 2:
            logger.debug('Less than 2 mergeable fields available. Nothing to merge.')
            return {}

        # Explain the feature using the user's own fields when a likely group exists
        cid_print('\n<BOLD>(Optional) Merge tag and account columns into single dashboard dimensions<END>')
        cid_print(
            '  The same business dimension (like application or team) can arrive as separate\n'
            '  columns: resource tags, IAM principal tags, account tags or account mapping columns.\n'
            '  Merging combines them into one column for dashboard filters and group by.'
        )
        example = self._find_example_group()
        if example:
            cid_print(f"  For example, these look like the same dimension: {', '.join(f'<BOLD>{f}<END>' for f in example)}")
        cid_print('  Skipping this step keeps all columns available individually.')
        cid_print('')

        if not get_yesno_parameter(
            param_name='taxonomy-merge-create',
            message='Merge some of these columns into single dashboard dimensions?',
            default='no',
        ):
            set_parameters({PARAM_NAME: ''})
            unset_parameter('taxonomy-merge-create')
            return {}

        merges = {}
        index = 1
        while True:
            # fields already used by a previous merge group are not offered again
            used_fields = set(field for fields in merges.values() for field in fields)
            remaining_fields = [field for field in self.available_fields if field not in used_fields]
            if len(remaining_fields) < 2:
                cid_print('Not enough fields left to create another merged column.')
                break
            fields = get_parameter(
                f'taxonomy-merge-{index}-fields',
                message=(
                    'Select fields for one merged column. The order IS the priority: '
                    'the first field that has a value wins '
                    '(hint: put the most specific source first, typically resource tags)'
                ),
                choices=self.grouped_choices(remaining_fields),
                multi=True,
                order=True,
            )
            fields = [field for field in (fields or []) if field in remaining_fields]
            if len(fields) < 2:
                cid_print('Select at least 2 fields to create a merged column. Skipping this group.')
            else:
                default_name = self.suggest_name(fields, taken=merges.keys())
                while True:
                    name = sanitize_name(get_parameter(
                        f'taxonomy-merge-{index}-name',
                        message='Enter a name for the merged column',
                        default=default_name,
                    ) or default_name)
                    if name in self.available_fields or name in merges:
                        cid_print(f'The dataset already has a column named <BOLD>{name}<END>. Please enter a different name.')
                        unset_parameter(f'taxonomy-merge-{index}-name')
                        continue
                    break
                merges[name] = fields
                cid_print('  ' + self.describe_merge(name, fields))
            remaining_after = len(remaining_fields) - len(fields)
            if remaining_after < 2 or not get_yesno_parameter(
                param_name=f'taxonomy-merge-{index}-more',
                message=f'Add another merged column? ({remaining_after} fields remaining)',
                default='no',
            ):
                break
            index += 1

        # Persist so that other datasets in this run and future runs reuse the same choice
        serialized = serialize_merges(merges)
        set_parameters({PARAM_NAME: serialized})
        # cleanup intermediate prompts so they do not pollute saved parameters
        unset_parameter('taxonomy-merge-create')
        for i in range(1, index + 1):
            unset_parameter(f'taxonomy-merge-{i}-fields')
            unset_parameter(f'taxonomy-merge-{i}-name')
            unset_parameter(f'taxonomy-merge-{i}-more')
        if serialized:
            cid_print('\n<BOLD>Merged columns:<END>')
            for name, fields in merges.items():
                cid_print('  ' + self.describe_merge(name, fields))
            cid_print(
                '  Merged columns become available in dashboard datasets. To use them as\n'
                '  dashboard filters and group by fields, select them in the taxonomy\n'
                '  fields question when asked (or re-deploy with --taxonomy to change it).'
            )
            cid_print(f"  Use <BOLD>--taxonomy-merges '{serialized}'<END> next time to skip these questions.")
        return merges

    def _find_example_group(self):
        """ Find a set of fields from different sources sharing their last name token
        (usually the dimension name: ..._application), for the explainer.
        Returns a list of 2+ field names or None. """
        by_token = {}
        for field in self.available_fields:
            tokens = field_tokens(field)
            if tokens:
                by_token.setdefault(tokens[-1], []).append(field)
        best = None
        for token, fields in by_token.items():
            if len(set(self.source_label(f) for f in fields)) > 1:
                if best is None or len(fields) > len(best):
                    best = fields
        return best
