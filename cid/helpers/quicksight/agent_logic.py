""" Pure logic for the Quick Agent platform (create-agent / list-agents / delete-agent).

This module is the deterministic, side-effect-free layer of the agent flow platform.
It MUST NOT import boto3/botocore or perform any I/O so that every function here can be
covered cheaply by property-based tests (pytest + Hypothesis) and reused by the
Space/Agent helpers and the Cid command handlers.
"""
import logging
from collections import Counter

from cid.exceptions import CidError

logger = logging.getLogger(__name__)

# Partition -> console domain mapping, mirroring CidBase.domain (cid/base.py)
PARTITION_DOMAINS = {
    'aws': 'aws.amazon.com',
    'aws-cn': 'amazonaws.cn',
    'aws-us-gov': 'amazonaws-us-gov.com',
}

# Console path templates per resource kind. Dashboards keep the existing
# Cid.qs_url shape (common.py); gen-AI resources are account-scoped.
_CONSOLE_PATHS = {
    'dashboard': 'sn/dashboards/{resource_id}',
    'agent': 'sn/account/{account_id}/agents/{resource_id}',
    'space': 'sn/account/{account_id}/spaces/{resource_id}',
}


def build_console_url(account_id: str, region: str, partition: str, domain: str, resource_kind: str, resource_id: str) -> str:
    """Build the partition/domain-correct QuickSight console URL for a resource.

    Used by create-agent to print the agent console URL with the
    partition and domain derived from the caller identity.

    :param account_id: AWS account id (used for account-scoped gen-AI resource paths)
    :param region: AWS region (e.g. ``us-east-1``, ``cn-north-1``)
    :param partition: AWS partition (``aws`` / ``aws-cn`` / ``aws-us-gov``)
    :param domain: console domain (e.g. ``aws.amazon.com``); when falsy, derived from partition
    :param resource_kind: one of ``dashboard`` / ``agent`` / ``space`` (plural accepted)
    :param resource_id: the resource id to link to
    :returns: the full https console URL
    :raises ValueError: on an unsupported resource kind
    """
    if not domain:
        domain = PARTITION_DOMAINS.get(partition, PARTITION_DOMAINS['aws'])
    kind = str(resource_kind or '').strip().lower()
    if kind.endswith('s'):
        kind = kind[:-1]
    if kind not in _CONSOLE_PATHS:
        raise ValueError(f'Unsupported resource kind {resource_kind!r}. Supported kinds: {sorted(_CONSOLE_PATHS)}.')
    path = _CONSOLE_PATHS[kind].format(account_id=account_id, resource_id=resource_id)
    return f'https://{region}.quicksight.{domain}/{path}'


# Official Agent API caps enforced by validate_caps
MAX_STARTER_PROMPTS = 3
MAX_STARTER_PROMPT_LEN = 100
MAX_WELCOME_MESSAGE_LEN = 300
MAX_NAME_LEN = 50
MIN_PERSONA_FIELD_LEN = 5
MAX_PERSONA_FIELD_LEN = 350000
MAX_SPACES = 10
MAX_ACTION_CONNECTORS = 10

# The five Persona fields required by the Agent API
PERSONA_FIELDS = ('identity', 'customInstructions', 'tone', 'outputStyle', 'responseLength')

# Manifest fields that must be present for create-agent to proceed
REQUIRED_MANIFEST_FIELDS = ('name', 'agentId', 'persona')


def validate_caps(manifest: dict) -> None:
    """Validate agent manifest fields against the official Agent API caps.

    Called by create-agent BEFORE any CreateAgent/UpdateAgent call so that cap
    violations surface as clear, actionable errors instead of opaque API
    rejections. Every error names the offending field.

    Expected manifest shape (see the agent bundle data model in the design)::

        {
            'name': 'CID FinOps Advisor',          # required, 1-50 chars, not whitespace-only
            'agentId': 'cid-finops-advisor',       # required
            'persona': {                           # required; personaFile resolved relative to the manifest source
                'identity': '...',                 # each of the 5 fields: 5-350000 chars
                'customInstructions': '...',
                'tone': '...',
                'outputStyle': '...',
                'responseLength': '...',
            },
            'starterPrompts': ['...'],             # optional, 0-3 strings, each <=100 chars
            'welcomeMessage': '...',               # optional, <=300 chars
            'dependsOn': {                         # optional
                'spaces': [...],                   # <=10 entries
                'actionConnectors': [...],         # <=10 entries
            },
        }

    :param manifest: the agent manifest dict (persona already resolved inline)
    :raises CidError: naming the offending field on any cap violation
    """
    if not isinstance(manifest, dict):
        raise CidError('Agent manifest must be a mapping, got {type_name}.'.format(type_name=type(manifest).__name__))

    # Required fields
    for field in REQUIRED_MANIFEST_FIELDS:
        if manifest.get(field) is None:
            raise CidError(f'Agent manifest is missing the required field {field!r}.')

    # Agent Name: non-empty, not whitespace-only, <=50 chars
    name = str(manifest['name'])
    if not name.strip():
        raise CidError("Agent 'name' must not be empty or whitespace-only.")
    if len(name) > MAX_NAME_LEN:
        raise CidError(f"Agent 'name' exceeds {MAX_NAME_LEN} characters (got {len(name)}).")

    # Starter prompts: at most 3, each <=100 chars
    starter_prompts = manifest.get('starterPrompts') or []
    if len(starter_prompts) > MAX_STARTER_PROMPTS:
        raise CidError(f"'starterPrompts' allows at most {MAX_STARTER_PROMPTS} prompts (got {len(starter_prompts)}).")
    for index, prompt in enumerate(starter_prompts):
        if len(str(prompt)) > MAX_STARTER_PROMPT_LEN:
            raise CidError(
                f"'starterPrompts[{index}]' exceeds {MAX_STARTER_PROMPT_LEN} characters (got {len(str(prompt))})."
            )

    # Welcome message: <=300 chars
    welcome_message = manifest.get('welcomeMessage')
    if welcome_message is not None and len(str(welcome_message)) > MAX_WELCOME_MESSAGE_LEN:
        raise CidError(
            f"'welcomeMessage' exceeds {MAX_WELCOME_MESSAGE_LEN} characters (got {len(str(welcome_message))})."
        )

    # Persona: all 5 fields present, each 5-350000 chars
    persona = manifest['persona']
    if not isinstance(persona, dict):
        raise CidError("Agent manifest field 'persona' must be a mapping of the 5 persona fields.")
    for field in PERSONA_FIELDS:
        value = persona.get(field)
        if value is None:
            raise CidError(f"Agent persona is missing the required field {field!r}.")
        length = len(str(value))
        if length < MIN_PERSONA_FIELD_LEN or length > MAX_PERSONA_FIELD_LEN:
            raise CidError(
                f'Persona field {field!r} must be between {MIN_PERSONA_FIELD_LEN} and '
                f'{MAX_PERSONA_FIELD_LEN} characters (got {length}).'
            )

    # Attached Spaces / action connectors: at most 10 each
    depends_on = manifest.get('dependsOn') or {}
    spaces = depends_on.get('spaces') or []
    if len(spaces) > MAX_SPACES:
        raise CidError(f"'dependsOn.spaces' allows at most {MAX_SPACES} Spaces per agent (got {len(spaces)}).")
    action_connectors = depends_on.get('actionConnectors') or []
    if len(action_connectors) > MAX_ACTION_CONNECTORS:
        raise CidError(
            f"'dependsOn.actionConnectors' allows at most {MAX_ACTION_CONNECTORS} "
            f'action connectors per agent (got {len(action_connectors)}).'
        )


def classify_dependencies(required: list, optional: list, present_keys) -> dict:
    """Classify dependency dashboard/dataset keys by deployment presence.

    Pure, deploy-free classification used by create-agent:
    the handler pre-flights the read-only ``ListDashboards`` call, builds the set of
    present catalog keys, and this function partitions the declared dependencies. The
    handler then decides: zero present -> guidance error; >=1 present -> proceed with
    the present set and warn per missing required/optional key.

    :param required: list of Required_Dashboard catalog keys (``dependsOn.dashboards``)
    :param optional: list of Optional_Dashboard catalog keys (``dependsOn.optionalDashboards``)
    :param present_keys: iterable of catalog keys confirmed deployed
    :returns: ``{'present': [...], 'missing_required': [...], 'missing_optional': [...]}``
        where ``present`` preserves declaration order (required first, then optional),
        ``missing_required`` preserves the ``required`` order and ``missing_optional``
        preserves the ``optional`` order; each key appears at most once per list
    """
    present_set = set(present_keys or ())
    present = []
    missing_required = []
    missing_optional = []
    seen = set()
    for key in list(required or []) + list(optional or []):
        if key in seen:
            continue
        seen.add(key)
        if key in present_set:
            present.append(key)
    seen = set()
    for key in required or []:
        if key in seen or key in present_set:
            continue
        seen.add(key)
        missing_required.append(key)
    seen = set()
    for key in optional or []:
        if key in seen or key in present_set:
            continue
        seen.add(key)
        missing_optional.append(key)
    return {
        'present': present,
        'missing_required': missing_required,
        'missing_optional': missing_optional,
    }


# The five Persona fields as the API returns them on read (Agent.CustomPromptInterface).
# The catalog persona.yaml uses camelCase (see PERSONA_FIELDS above); persona_differs
# normalizes both spellings so either side can use either convention.
PERSONA_API_FIELDS = ('Identity', 'CustomInstructions', 'Tone', 'OutputStyle', 'ResponseLength')


def _normalize_persona(persona: dict) -> dict:
    """Map a persona dict to {lowercased field name: string value} for the 5 fields only.

    Field names differ between the catalog (camelCase, e.g. ``customInstructions``) and
    the API read shape (PascalCase, e.g. ``CustomInstructions``) only by letter case, so
    case-insensitive key matching handles both. Any other key (notably the
    server-derived ``promptSummary`` / ``PromptSummary``) is dropped from the comparison.
    """
    wanted = {field.lower(): field for field in PERSONA_API_FIELDS}
    normalized = {}
    for key, value in (persona or {}).items():
        lowered = str(key).lower()
        if lowered in wanted and value is not None:
            normalized[lowered] = str(value)
    return normalized


def persona_differs(desired: dict, deployed: dict) -> bool:
    """Return True if the desired persona differs from the deployed persona.

    Compares ONLY the five persona fields (``Identity``, ``CustomInstructions``,
    ``Tone``, ``OutputStyle``, ``ResponseLength``) and EXCLUDES the server-generated
    ``promptSummary`` and any other server-derived key. Keys are matched
    case-insensitively per field so a camelCase catalog persona (``identity``, ...)
    compares correctly against the PascalCase API read shape
    (``Agent.CustomPromptInterface``: ``Identity``, ...). Values are compared as exact
    strings; a field present on one side but absent (or None) on the other counts as a
    difference.

    :param desired: persona from the catalog definition (camelCase or PascalCase keys)
    :param deployed: persona from the API read (``Agent.CustomPromptInterface``)
    :returns: True when at least one of the five fields differs in value or presence
    """
    return _normalize_persona(desired) != _normalize_persona(deployed)


def compute_space_delta(desired, current) -> tuple:
    """Compute the add/remove delta for Space or action-connector associations.

    Used by the Agent update path to send delta-based add/remove lists instead of a
    full replacement: applying ``(to_add, to_remove)`` to ``current`` yields exactly
    ``desired``.

    :param desired: iterable of desired ARNs
    :param current: iterable of currently associated ARNs
    :returns: tuple ``(to_add, to_remove)`` of sets: ``(desired - current, current - desired)``
    """
    desired_set = set(desired or ())
    current_set = set(current or ())
    return (desired_set - current_set, current_set - desired_set)


def describe_drift(definition: dict, deployed: dict) -> list:
    """List the catalog-managed fields whose values differ from the deployed agent.

    Compares the catalog definition against the DescribeAgent read shape and names
    each drifted field so the user can see WHAT an update would change before
    consenting to it. Fields the definition does not carry (None) are not managed
    and never count as drift. Space associations are resolved later in the flow and
    are not part of this report; ``lifecycle`` is create-only and excluded.

    :param definition: agent definition from the catalog (same shape create_or_update takes)
    :param deployed: the DescribeAgent read shape of the deployed agent
    :returns: list of human-readable drifted-field names, e.g.
        ``['persona.Identity', 'welcomeMessage']``; empty when nothing managed differs
    """
    drift = []
    name = definition.get('name')
    if name is not None and name != (deployed.get('Name') or deployed.get('AgentName')):
        drift.append('name')
    desired_persona = definition.get('persona') or {}
    if desired_persona:
        desired_normalized = _normalize_persona(desired_persona)
        deployed_normalized = _normalize_persona(deployed.get('CustomPromptInterface') or {})
        for field in PERSONA_API_FIELDS:
            if desired_normalized.get(field.lower()) != deployed_normalized.get(field.lower()):
                drift.append(f'persona.{field}')
    starter_prompts = definition.get('starterPrompts')
    if starter_prompts is not None and list(starter_prompts) != list(deployed.get('StarterPrompts') or ()):
        drift.append('starterPrompts')
    welcome_message = definition.get('welcomeMessage')
    if welcome_message is not None and str(welcome_message) != str(deployed.get('WelcomeMessage') or ''):
        drift.append('welcomeMessage')
    desired_connectors = set((definition.get('dependsOn') or {}).get('actionConnectors') or ())
    connectors_to_add, connectors_to_remove = compute_space_delta(
        desired_connectors, deployed.get('ActionConnectors') or ())
    if connectors_to_add or connectors_to_remove:
        drift.append('actionConnectors')
    return drift


def compute_space_additions(current_arns, desired_arns) -> set:
    """Compute the additive, de-duplicated set of resource ARNs to add to a Space.

    Space resource updates are strictly additive: only resources
    not already in the Space are added, de-duplicated by ARN, and nothing is removed
    here (removal is the separate, opt-in :func:`compute_stale_removals`).

    :param current_arns: iterable of ARNs currently in the Space
    :param desired_arns: iterable of ARNs the agent's dependencies resolve to (may repeat)
    :returns: set of ARNs to add (``desired - current``); a set is returned because
        UpdateSpaceResources ordering is not meaningful and set semantics guarantee
        de-duplication
    """
    return set(desired_arns or ()) - set(current_arns or ())


def compute_stale_removals(space_arns, referenced_arns, managed_arns, remove_stale) -> set:
    """Compute the opt-in, scoped set of stale Space resources to remove.

    Removal is opt-in (``--cleanup-space``) and scoped: only resources
    that are CID-managed AND no longer referenced by any agent's dependencies AND
    actually present in the Space are ever removed. Resources another provider added
    (not CID-managed) are never touched.

    :param space_arns: iterable of ARNs currently in the Space
    :param referenced_arns: iterable of ARNs still referenced by any agent's dependencies
    :param managed_arns: iterable of ARNs known to be CID-managed
    :param remove_stale: the ``--cleanup-space`` flag; falsy -> remove nothing
    :returns: set of ARNs to remove; empty when ``remove_stale`` is falsy, else exactly
        ``space ∩ managed − referenced``
    """
    if not remove_stale:
        return set()
    return (set(space_arns or ()) & set(managed_arns or ())) - set(referenced_arns or ())


# UpdateAgent reports partial association failures IN-BAND (response lists), not as
# exceptions: an HTTP 200 can still carry per-ARN add/remove failures. Each list
# entry is a structure with 'Arn', 'ErrorMessage' and 'ErrorCode'.
ASSOCIATION_FAILURE_FIELDS = (
    ('FailedToAddSpaces', 'add Space'),
    ('FailedToRemoveSpaces', 'remove Space'),
    ('FailedToAddActionConnectors', 'add action connector'),
    ('FailedToRemoveActionConnectors', 'remove action connector'),
)


def association_failures(response) -> list:
    """Extract UpdateAgent partial association failures as readable strings.

    UpdateAgent can succeed (HTTP 200) while individual Space or action-connector
    add/remove operations fail; those failures are reported only in the response
    lists (:data:`ASSOCIATION_FAILURE_FIELDS`). Ignoring them leaves the deployed
    associations silently diverged from the requested ones, so every caller that
    sends association deltas must check this.

    :param response: the UpdateAgent response dict; None and non-dict values are
        tolerated (treated as carrying no failures), as are absent or non-list fields
    :returns: list of human-readable failure strings, one per failed ARN, e.g.
        ``failed to add Space arn:... (AccessDenied: not authorized)``; empty when
        every association operation succeeded
    """
    failures = []
    if not isinstance(response, dict):
        return failures
    for field, operation in ASSOCIATION_FAILURE_FIELDS:
        entries = response.get(field)
        if not isinstance(entries, (list, tuple)):
            continue
        for entry in entries:
            if isinstance(entry, dict):
                arn = entry.get('Arn') or '<unknown ARN>'
                detail = ': '.join(str(part) for part in (entry.get('ErrorCode'), entry.get('ErrorMessage')) if part)
                failures.append(f'failed to {operation} {arn}' + (f' ({detail})' if detail else ''))
            else:
                failures.append(f'failed to {operation} {entry}')
    return failures


# CID_Managed provenance: dual mechanism.
# (a) resource tag written via TagResource (tolerated failure), and
# (b) marker string embedded in the resource Description — the existing
#     'Created by Cloud Intelligence Dashboards' convention that
#     QuickSight.ensure_group_exists already uses for the cid-owners group.
CID_PROVENANCE_TAG_KEY = 'cid_managed'
CID_PROVENANCE_TAG_VALUE = 'true'
CID_MANAGED_MARKER = 'Created by Cloud Intelligence Dashboards'


def is_cid_managed(tags, description) -> bool:
    """Dual-mechanism CID_Managed provenance detection.

    A resource is CID-managed when EITHER the CID provenance tag
    (:data:`CID_PROVENANCE_TAG_KEY`) is present in its tags OR its description contains
    the CID-managed marker (:data:`CID_MANAGED_MARKER`, exact case-sensitive substring).
    Either mechanism alone suffices because the tag write is tolerated-on-failure while
    the Description marker is always written.

    :param tags: resource tags as either a mapping ``{key: value}`` or the raw API list
        shape ``[{'Key': ..., 'Value': ...}, ...]``; None/empty accepted. Tag-key match
        is exact; presence of the key counts regardless of its value.
    :param description: the resource Description string (None/empty accepted)
    :returns: True when the provenance tag is present or the description carries the marker
    """
    tag_keys = set()
    if isinstance(tags, dict):
        tag_keys = {str(key) for key in tags}
    elif tags:
        for tag in tags:
            if isinstance(tag, dict) and 'Key' in tag:
                tag_keys.add(str(tag['Key']))
    if CID_PROVENANCE_TAG_KEY in tag_keys:
        return True
    return bool(description) and CID_MANAGED_MARKER in str(description)


# SpaceId pattern characters (see the SpaceId pattern [0-9a-zA-Z-_=.+]+)
_SPACE_ID_ALLOWED_CHARS = frozenset(
    '0123456789'
    'abcdefghijklmnopqrstuvwxyz'
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    '-_=.+'
)
# Deterministic fallback when sanitization would produce an empty id
_SPACE_ID_FALLBACK = 'cid-space'


def derive_space_id(name_or_id: str) -> str:
    """Derive a valid QuickSight space id deterministically from a name or id.

    Sanitization rules (deterministic: the same input always yields the same output):

    1. The input is stringified; if it already fully matches the SpaceId pattern
       ``[0-9a-zA-Z-_=.+]+`` it is returned unchanged (catalog space ids pass through).
    2. Otherwise every maximal run of invalid characters (anything outside
       ``0-9 a-z A-Z - _ = . +``, e.g. spaces or unicode) is replaced by a single ``-``.
    3. Hyphens introduced at the start or end by step 2 are stripped.
    4. If the result is empty (input had no valid characters),
       :data:`_SPACE_ID_FALLBACK` is returned so the id is never empty.

    :param name_or_id: a space name (bring-your-own case) or catalog space id
    :returns: a non-empty space id matching ``[0-9a-zA-Z-_=.+]+``
    """
    text = str(name_or_id or '')
    if text and all(char in _SPACE_ID_ALLOWED_CHARS for char in text):
        return text
    parts = []
    run_invalid = False
    for char in text:
        if char in _SPACE_ID_ALLOWED_CHARS:
            if run_invalid and parts:
                parts.append('-')
            run_invalid = False
            parts.append(char)
        else:
            run_invalid = True
    derived = ''.join(parts)
    return derived or _SPACE_ID_FALLBACK


# Check indicator used to mark deployed entries in listings/pickers, mirroring the
# existing dashboard picker in common.py
DEPLOYED_MARK = '✓'
# Category hidden from listings and pickers
HIDDEN_CATEGORY = 'Deprecated'
# Category used when an entry declares none, mirroring the dashboard picker default
DEFAULT_CATEGORY = 'Other'


def build_agent_listing(agents_catalog: dict, deployed_ids) -> dict:
    """Build the category-grouped agent listing / picker structure.

    Groups catalog agent entries by ``category``, hides entries in category
    ``Deprecated``, and marks entries whose catalog key or ``agentId`` is in
    ``deployed_ids`` with the check indicator (:data:`DEPLOYED_MARK`). Each catalog
    entry appears exactly once, under its own category.

    Returned shape (suitable for both ``list-agents`` output and picker choices)::

        {
            '<category>': [
                {
                    'key': '<catalog key>',
                    'agentId': '<agentId or key>',
                    'name': '<display name or key>',
                    'deployed': bool,
                    'display': ' ✓[<agentId>] <name>',   # ' ' instead of ✓ when not deployed
                },
                ...
            ],
        }

    Categories and entries preserve catalog iteration order.

    :param agents_catalog: mapping of catalog key -> agent entry dict (``category``,
        ``name``, ``agentId`` all optional; ``agentId`` defaults to the key)
    :param deployed_ids: iterable of deployed agent ids and/or catalog keys
    :returns: dict of category -> list of entry dicts as documented above
    """
    deployed_set = set(deployed_ids or ())
    listing = {}
    for key, entry in (agents_catalog or {}).items():
        entry = entry or {}
        category = str(entry.get('category') or DEFAULT_CATEGORY)
        if category == HIDDEN_CATEGORY:
            continue
        agent_id = entry.get('agentId') or key
        name = entry.get('name') or key
        deployed = key in deployed_set or agent_id in deployed_set
        check = DEPLOYED_MARK if deployed else ' '
        listing.setdefault(category, []).append({
            'key': key,
            'agentId': agent_id,
            'name': name,
            'deployed': deployed,
            'display': f' {check}[{agent_id}] {name}',
        })
    return listing


def select_database_from_candidates(dataset_databases) -> str | None:
    """Select the Athena database for the agent Parameter_Store from dataset candidates.

    Pure selection kernel for the dataset-based database inference (step 2b of the
    agent-command resolution chain): the agent commands collect the Athena database
    referenced by each deployed CID dataset's physical table map
    (``PhysicalTableMap -> RelationalTable -> Schema``, one entry per referencing
    dataset) and this function picks one deterministically — the database referenced
    by the greatest number of datasets, breaking ties by ascending lexicographic
    order of database name. The same input always yields the same output.

    :param dataset_databases: iterable of database names discovered from dataset
        physical table maps (may repeat; one entry per referencing dataset);
        None/empty entries are ignored
    :returns: the selected database name, or None when no candidate is available
    """
    counts = Counter(str(database) for database in (dataset_databases or ()) if database)
    if not counts:
        return None
    return min(counts, key=lambda name: (-counts[name], name))
