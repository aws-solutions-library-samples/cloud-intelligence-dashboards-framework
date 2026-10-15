""" Pure logic for the Quick Agent platform (create-agent / list-agents / delete-agent).

This module is the deterministic, side-effect-free layer of the agent flow platform.
It MUST NOT import boto3/botocore or perform any I/O so that every function here can be
covered cheaply by property-based tests (pytest + Hypothesis) and reused by the
Space/Agent helpers and the Cid command handlers.
"""
import time
import logging

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
    """Quick console URL of a dashboard, agent or space for the given partition and region."""
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
    """Validate an agent definition against the Agent API limits before any API call; raise CidError naming the field."""
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


def classify_dependencies(dashboards: list, present_keys) -> dict:
    """Split dependency dashboard ids into ``{'present': [...], 'missing': [...]}`` (declaration order, no duplicates)."""
    present_set = set(present_keys or ())
    present, missing, seen = [], [], set()
    for key in dashboards or []:
        if key in seen:
            continue
        seen.add(key)
        (present if key in present_set else missing).append(key)
    return {'present': present, 'missing': missing}


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
    """True when the five persona fields differ (keys matched case-insensitively; server-derived keys ignored)."""
    return _normalize_persona(desired) != _normalize_persona(deployed)


def compute_space_delta(desired, current) -> tuple:
    """Delta between desired and current associations: ``(desired - current, current - desired)``."""
    desired_set = set(desired or ())
    current_set = set(current or ())
    return (desired_set - current_set, current_set - desired_set)


def describe_drift(definition: dict, deployed: dict, override: bool = False) -> list:
    """Names of the catalog-managed agent fields that differ from the deployed agent (for the update prompt).
    Extra action connectors count only with ``override`` (``--on-drift override``)."""
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
    if connectors_to_add or (override and connectors_to_remove):
        drift.append('actionConnectors')
    return drift


def compute_space_additions(current_arns, desired_arns) -> set:
    """ARNs to add to a Space: desired minus current (additive, de-duplicated)."""
    return set(desired_arns or ()) - set(current_arns or ())


# Space resource types the agent catalog declares (dashboards, datasets, knowledge
# bases); --on-drift override only ever removes resources of these types.
SPACE_MANAGED_RESOURCE_TYPES = ('DASHBOARD', 'DATA_SET', 'KNOWLEDGE_BASE')


def compute_space_removals(current_resources: dict, desired_arns) -> dict:
    """Space resources ``--on-drift override`` removes: catalog-managed types (dashboards, datasets,
    knowledge bases) not in the desired set. Other resource types are never touched."""
    desired = set(desired_arns or ())
    return {
        arn: resource_type
        for arn, resource_type in (current_resources or {}).items()
        if resource_type in SPACE_MANAGED_RESOURCE_TYPES and arn not in desired
    }


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
    """Readable list of the Space/connector failures UpdateAgent reports in-band (HTTP 200)."""
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


def retry_on_conflict(action, conflict_exception, timeout: int=300, interval: int=5):
    """Call ``action()``, retrying every ``interval`` seconds while it raises
    ``conflict_exception`` (a resource still settling), up to ``timeout`` seconds;
    then the last conflict is re-raised. Returns what ``action`` returns."""
    elapsed = 0
    while True:
        try:
            return action()
        except conflict_exception:
            if elapsed >= timeout:
                raise
            logger.debug(f'Resource busy; retrying in {interval}s ({elapsed}/{timeout}s elapsed).')
            time.sleep(interval)
            elapsed += interval


def poll(fetch, done, timeout: int=300, interval: int=5):
    """Call ``fetch()`` every ``interval`` seconds until ``done(result)`` is true or
    ``timeout`` seconds passed. Returns ``(result, finished)``."""
    elapsed = 0
    while True:
        result = fetch()
        if done(result):
            return result, True
        if elapsed >= timeout:
            return result, False
        time.sleep(interval)
        elapsed += interval


# CID_Managed provenance: dual mechanism.
# (a) resource tag written via TagResource (tolerated failure), and
# (b) marker string embedded in the resource Description — the existing
#     'Created by Cloud Intelligence Dashboards' convention that
#     QuickSight.ensure_group_exists already uses for the cid-owners group.
CID_PROVENANCE_TAG_KEY = 'cid_managed'
CID_PROVENANCE_TAG_VALUE = 'true'
CID_MANAGED_MARKER = 'Created by Cloud Intelligence Dashboards'


def is_cid_managed(tags, description) -> bool:
    """True when the CID provenance tag is present or the description carries the CID-managed marker."""
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
    """Deterministic, valid Space id from a name or id (invalid character runs become one dash)."""
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
    """Catalog agents grouped by category (Deprecated hidden), each with key, agentId, name, deployed and
    a ``' ✓[agentId] name'`` display label."""
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
