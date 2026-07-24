"""Cross-dimension tag merging for CUR2.

When a customer uses the same tag key across multiple dimensions (resource tags,
account tags, IAM principal tags, user attributes, cost categories), this module
generates COALESCE SQL expressions that unify them into a single field in the
tags_json output.

Example: A customer with tag "application" on both account level and resource level
gets a unified "application" field that prefers the resource-level value but falls
back to the account-level value when not present.

Only CUR2 supports MAP-based tag columns; CUR1 flat columns are not eligible.
"""
import logging
import re
from typing import Dict, List, Optional, Set, Tuple

from cid.utils import (
    cid_print,
    get_parameter,
    get_parameters,
    isatty,
    set_parameters,
)

logger = logging.getLogger(__name__)

# Dimension priority orders for COALESCE expression generation.
# resource_first: prefer granular resource-level tagging over account-level defaults.
STRATEGIES: Dict[str, List[str]] = {
    'resource_first': [
        'resource_tags',
        'iam_principal',
        'user_attribute',
        'cost_category',
        'account_tags',
    ],
    'account_first': [
        'account_tags',
        'resource_tags',
        'iam_principal',
        'user_attribute',
        'cost_category',
    ],
}

DIMENSION_LABELS: Dict[str, str] = {
    'resource_tags': 'Resource Tag',
    'account_tags': 'Account Tag',
    'iam_principal': 'IAM Principal Tag',
    'user_attribute': 'User Attribute Tag',
    'cost_category': 'Cost Category',
}


def normalise_key(selector: str) -> str:
    """Extract the bare, canonical tag key from any CUR2 selector.

    All dimension-specific prefixes are stripped and the result is lowercased
    with non-word characters replaced by underscores.

    Args:
        selector: A CUR2 MAP accessor string or CUR1 flat column name.

    Returns:
        Normalised bare key suitable for cross-dimension comparison.

    Examples:
        >>> normalise_key("resource_tags['user_environment']")
        'environment'
        >>> normalise_key("tags['accountTag/Environment']")
        'environment'
        >>> normalise_key("tags['iamPrincipal/Team']")
        'team'
        >>> normalise_key("tags['userAttribute/Department']")
        'department'
        >>> normalise_key("cost_category['BillingTeam']")
        'billing_team'
        >>> normalise_key("line_item_iam_principal")
        'iam_principal'
    """
    if selector == 'line_item_iam_principal':
        return 'iam_principal'

    # Extract key from MAP accessor: foo['bar'] → bar
    if "['" in selector:
        key = selector.split("['")[1].split("']")[0]
    else:
        # CUR1 flat column: resource_tags_user_environment → user_environment
        key = selector
        for flat_prefix in ('resource_tags_', 'cost_category_'):
            if key.startswith(flat_prefix):
                key = key[len(flat_prefix):]
                break

    # Strip dimension prefixes
    for prefix in ('accountTag/', 'userAttribute/', 'iamPrincipal/', 'user_'):
        if key.startswith(prefix):
            key = key[len(prefix):]
            break

    # CamelCase/PascalCase → snake_case (e.g. BusinessUnit → business_unit)
    key = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', '_', key)

    # Normalise: lowercase, non-word chars → underscore
    return re.sub(r'\W', '_', key).lower()


def selector_dimension(selector: str) -> str:
    """Classify a CUR2 tag selector into its dimension category.

    Args:
        selector: A CUR2 MAP accessor string.

    Returns:
        One of: 'resource_tags', 'account_tags', 'iam_principal',
        'user_attribute', 'cost_category'.
    """
    if selector == 'line_item_iam_principal':
        return 'iam_principal'
    if selector.startswith('resource_tags'):
        return 'resource_tags'
    if selector.startswith('cost_category'):
        return 'cost_category'
    if 'accountTag/' in selector:
        return 'account_tags'
    if 'iamPrincipal/' in selector:
        return 'iam_principal'
    if 'userAttribute/' in selector:
        return 'user_attribute'
    return 'resource_tags'


class TagMerger:
    """Discovers and generates SQL for cross-dimension tag merges.

    Usage::

        merger = TagMerger(cur.tag_and_cost_category_fields)
        if merger.merge_candidates:
            keys_to_merge, strategy = merger.resolve_merge_keys()
            tags_and_names = merger.inject_merged_entries(
                tags_and_names, keys_to_merge, strategy
            )
    """

    def __init__(self, selectors: List[str], param_prefix: str = 'resource-tags'):
        """
        Args:
            selectors: All tag/cost_category selectors from CUR2 discovery.
            param_prefix: Parameter namespace for persisting merge choices.
        """
        self._selectors = selectors
        self._param_prefix = param_prefix
        self._merge_candidates: Optional[Dict[str, List[str]]] = None

    @property
    def merge_candidates(self) -> Dict[str, List[str]]:
        """Tags that exist in 2+ dimensions, keyed by normalised bare key.

        Only returns keys where the selectors span genuinely different dimensions
        (not just two resource tags with similar names).

        Returns:
            {bare_key: [selector1, selector2, ...]} for multi-dimension keys.
        """
        if self._merge_candidates is None:
            by_key: Dict[str, List[str]] = {}
            for sel in self._selectors:
                key = normalise_key(sel)
                by_key.setdefault(key, []).append(sel)

            # Keep only keys that span multiple distinct dimensions
            self._merge_candidates = {
                k: v for k, v in by_key.items()
                if len(set(selector_dimension(s) for s in v)) > 1
            }
        return self._merge_candidates

    def resolve_merge_keys(self) -> Tuple[Set[str], str]:
        """Determine which keys to merge and the strategy to use.

        Checks persisted config first, then prompts interactively if needed.
        Non-interactive sessions skip merging entirely.

        Returns:
            (keys_to_merge, strategy) tuple.
        """
        params = get_parameters()
        strategy = params.get(
            f'{self._param_prefix}-merge-strategy', 'resource_first'
        )

        # Check for persisted merge selection from a previous run
        cached = params.get(f'{self._param_prefix}-merge-tags')
        if cached is not None:
            if isinstance(cached, str):
                keys = {k for k in cached.split(',') if k}
            else:
                keys = set(cached)
            # Prune stale keys no longer in current CUR
            keys &= set(self.merge_candidates.keys())
            logger.info(
                f'Using persisted merge config: keys={sorted(keys)}, '
                f'strategy={strategy}'
            )
            return keys, strategy

        # Non-interactive: skip merging silently
        if not isatty():
            logger.info('Non-interactive session: skipping tag merge prompts.')
            return set(), strategy

        # No candidates: nothing to do
        if not self.merge_candidates:
            return set(), strategy

        # Display candidates table
        cid_print('\n<BOLD>Cross-dimension tag merge candidates:<END>')
        cid_print(
            '  These tags exist in multiple dimensions and can be unified.\n'
            '  The merged field uses the most specific value available\n'
            '  (resource tag > IAM principal > cost category > account tag).\n'
        )
        max_len = max(len(k) for k in self.merge_candidates)
        for key, sels in sorted(self.merge_candidates.items()):
            dims = ', '.join(
                DIMENSION_LABELS.get(selector_dimension(s), selector_dimension(s))
                for s in sorted(sels, key=lambda x: selector_dimension(x))
            )
            cid_print(f'  <BOLD>{key:<{max_len}}<END>  ←  {dims}')
        cid_print('')

        # Single prompt: multi-select which tags to merge
        chosen = get_parameter(
            f'{self._param_prefix}-merge-tags',
            message=(
                'Select tags to merge across dimensions '
                '(resource tag takes priority when values conflict)'
            ),
            multi=True,
            choices=sorted(self.merge_candidates.keys()),
            default=sorted(self.merge_candidates.keys()),
        )
        keys_to_merge = set(chosen) if chosen else set()

        # Persist for subsequent runs (cid update, cid deploy)
        set_parameters({
            f'{self._param_prefix}-merge-tags': ','.join(sorted(keys_to_merge)),
            f'{self._param_prefix}-merge-strategy': strategy,
        })

        logger.info(
            f'User selected merge keys: {sorted(keys_to_merge)}, '
            f'strategy: {strategy}'
        )
        return keys_to_merge, strategy

    def build_merge_expression(
        self, bare_key: str, strategy: str = 'resource_first'
    ) -> str:
        """Generate COALESCE(NULLIF(...,''), ...) SQL for a merged tag.

        Athena MAP element accessors return '' (empty string) rather than NULL
        for missing keys, so NULLIF wrapping is required for COALESCE to
        correctly fall through to the next dimension.

        Args:
            bare_key: Normalised tag key to merge.
            strategy: Priority order name from STRATEGIES.

        Returns:
            SQL expression string for embedding in MAP_FROM_ENTRIES ARRAY.

        Raises:
            ValueError: If bare_key has no merge candidates.
        """
        selectors = self.merge_candidates.get(bare_key)
        if not selectors:
            raise ValueError(f'No merge candidates for key: {bare_key!r}')

        order = STRATEGIES.get(strategy, STRATEGIES['resource_first'])

        def rank(sel: str) -> int:
            dim = selector_dimension(sel)
            try:
                return order.index(dim)
            except ValueError:
                return len(order)

        ordered = sorted(selectors, key=rank)
        logger.debug(
            f'Merge order for "{bare_key}": '
            f'{[selector_dimension(s) for s in ordered]}'
        )

        args = ', '.join(f"NULLIF({sel}, '')" for sel in ordered)
        return f'COALESCE({args})'

    def inject_merged_entries(
        self,
        tags_and_names: Dict[str, str],
        keys_to_merge: Set[str],
        strategy: str,
    ) -> Dict[str, str]:
        """Add merged tag entries to the tags_and_names mapping.

        For each key in keys_to_merge, adds an entry with the bare key name
        mapped to the COALESCE SQL expression. Individual per-dimension entries
        remain available for users who want granular breakdown alongside the
        unified view.

        Args:
            tags_and_names: {display_name: sql_selector} for all discovered tags.
            keys_to_merge: Set of bare keys the user chose to merge.
            strategy: Priority order for COALESCE generation.

        Returns:
            New dict with merged entries added (original dict is not modified).
        """
        result = dict(tags_and_names)
        for bare_key in sorted(keys_to_merge):
            if bare_key in self.merge_candidates:
                result[bare_key] = self.build_merge_expression(bare_key, strategy)
                logger.debug(
                    f'Injected merged entry: {bare_key} → {result[bare_key]}'
                )
        return result
