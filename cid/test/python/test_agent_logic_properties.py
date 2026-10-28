"""Property-based tests for the pure-logic agent module (cid/helpers/quicksight/agent_logic.py).

Feature: cid-cmd-agent-flow-platform
One Hypothesis property test per Correctness Property from the design document.
All tests run with at least 100 examples and never perform I/O.
"""
import re

import pytest
from hypothesis import given, settings, strategies as st

from cid.exceptions import CidError
from cid.helpers.quicksight.agent_logic import (
    CID_MANAGED_MARKER,
    CID_PROVENANCE_TAG_KEY,
    DEPLOYED_MARK,
    PARTITION_DOMAINS,
    PERSONA_API_FIELDS,
    PERSONA_FIELDS,
    build_agent_listing,
    build_console_url,
    classify_dependencies,
    compute_space_additions,
    compute_space_delta,
    compute_space_removals,
    SPACE_MANAGED_RESOURCE_TYPES,
    derive_space_id,
    is_cid_managed,
    persona_differs,
    validate_caps,
)

# ---------------------------------------------------------------------------
# Shared strategies
# ---------------------------------------------------------------------------

# Valid ${var} identifiers. Prefixes guarantee known/unknown never collide.
_ident_body = st.text(alphabet='abcdefghijklmnopqrstuvwxyz_', min_size=1, max_size=8)
_known_idents = _ident_body.map(lambda s: 'k' + s)
_unknown_idents = _ident_body.map(lambda s: 'u' + s)

# Literal text that cannot start a Template token ('$' excluded).
_literals = st.text(
    alphabet=st.characters(blacklist_characters='$', blacklist_categories=('Cs',)),
    max_size=20,
)

_dep_keys = st.text(alphabet='abcdefghijklmnopqrstuvwxyz0123456789-', min_size=1, max_size=12)
_arns = _dep_keys.map(lambda s: f'arn:aws:quicksight:us-east-1:123456789012:dashboard/{s}')
_arn_sets = st.frozensets(_arns, max_size=10)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 6: Console URLs are partition-correct
# Validates: Requirements 17.3
# ---------------------------------------------------------------------------
_REGIONS_BY_PARTITION = {
    'aws': ['us-east-1', 'eu-west-1', 'ap-southeast-2'],
    'aws-cn': ['cn-north-1', 'cn-northwest-1'],
    'aws-us-gov': ['us-gov-west-1', 'us-gov-east-1'],
}


# Feature: cid-cmd-agent-flow-platform, Property 6: Console URLs are partition-correct
@settings(max_examples=100, deadline=None)
@given(
    account_id=st.text(alphabet='0123456789', min_size=12, max_size=12),
    partition=st.sampled_from(sorted(PARTITION_DOMAINS)),
    region_index=st.integers(min_value=0, max_value=2),
    explicit_domain=st.one_of(st.none(), st.just(''), st.sampled_from(['aws.amazon.com', 'amazonaws.cn', 'amazonaws-us-gov.com'])),
    resource_kind=st.sampled_from(['dashboard', 'agent', 'dashboards', 'agents', 'Agent ', 'AGENT']),
    resource_id=st.text(alphabet='abcdefghijklmnopqrstuvwxyz0123456789-', min_size=1, max_size=20),
)
def test_property_6_console_urls_partition_correct(account_id, partition, region_index, explicit_domain, resource_kind, resource_id):
    """**Validates: Requirements 17.3**"""
    regions = _REGIONS_BY_PARTITION[partition]
    region = regions[region_index % len(regions)]
    url = build_console_url(account_id, region, partition, explicit_domain, resource_kind, resource_id)
    expected_domain = explicit_domain or PARTITION_DOMAINS[partition]
    # The URL host is partition/domain-correct and region-scoped
    assert url.startswith(f'https://{region}.quicksight.{expected_domain}/')
    # The resource id is present in the path
    assert resource_id in url
    # no account segment: the console resolves the account from the session
    assert '/sn/account/' not in url
    if resource_kind.strip().lower().rstrip('s') == 'agent':
        assert url.endswith(f'/sn/start/agents?view={resource_id}')
    # When no domain is supplied, the domain is derived purely from the partition
    if not explicit_domain:
        assert PARTITION_DOMAINS[partition] in url


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 7: Cap validation rejects exactly
# the out-of-bounds fields, including persona length bounds and Space/connector count caps
# Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7
# ---------------------------------------------------------------------------
_name_texts = st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ', min_size=1, max_size=50).filter(lambda s: s.strip())
_prompt_texts = st.text(max_size=100)

_VIOLATIONS = [
    ('name-empty', 'name'),
    ('name-whitespace', 'name'),
    ('name-too-long', 'name'),
    ('too-many-prompts', 'starterPrompts'),
    ('prompt-too-long', 'starterPrompts'),
    ('welcome-too-long', 'welcomeMessage'),
    ('missing-name', 'name'),
    ('missing-agentId', 'agentId'),
    ('missing-persona', 'persona'),
    ('persona-too-short', None),  # expected token filled in with the field name
    ('persona-too-long', None),
    ('too-many-spaces', 'spaces'),
    ('too-many-connectors', 'actionConnectors'),
]


@st.composite
def _cap_cases(draw):
    """Draw a valid manifest, then optionally inject exactly one cap violation."""
    manifest = {
        'name': draw(_name_texts),
        'agentId': draw(_dep_keys),
        'persona': {field: draw(st.text(min_size=5, max_size=80)) for field in PERSONA_FIELDS},
    }
    if draw(st.booleans()):
        manifest['starterPrompts'] = draw(st.lists(_prompt_texts, max_size=3))
    if draw(st.booleans()):
        manifest['welcomeMessage'] = draw(st.text(max_size=300))
    if draw(st.booleans()):
        manifest['dependsOn'] = {
            'spaces': draw(st.lists(_dep_keys, max_size=10)),
            'actionConnectors': draw(st.lists(_dep_keys, max_size=10)),
        }

    violation = draw(st.one_of(st.none(), st.sampled_from(_VIOLATIONS)))
    if violation is None:
        return manifest, None

    kind, expected_token = violation
    if kind == 'name-empty':
        manifest['name'] = ''
    elif kind == 'name-whitespace':
        manifest['name'] = ' ' * draw(st.integers(min_value=1, max_value=5))
    elif kind == 'name-too-long':
        manifest['name'] = 'a' * draw(st.integers(min_value=51, max_value=80))
    elif kind == 'too-many-prompts':
        manifest['starterPrompts'] = draw(st.lists(_prompt_texts, min_size=4, max_size=7))
    elif kind == 'prompt-too-long':
        prompts = draw(st.lists(_prompt_texts, max_size=2))
        prompts.append('p' * draw(st.integers(min_value=101, max_value=200)))
        manifest['starterPrompts'] = prompts
    elif kind == 'welcome-too-long':
        manifest['welcomeMessage'] = 'w' * draw(st.integers(min_value=301, max_value=400))
    elif kind == 'missing-name':
        manifest.pop('name')
    elif kind == 'missing-agentId':
        manifest['agentId'] = None
    elif kind == 'missing-persona':
        manifest.pop('persona')
    elif kind == 'persona-too-short':
        field = draw(st.sampled_from(PERSONA_FIELDS))
        manifest['persona'][field] = 'x' * draw(st.integers(min_value=0, max_value=4))
        expected_token = field
    elif kind == 'persona-too-long':
        field = draw(st.sampled_from(PERSONA_FIELDS))
        manifest['persona'][field] = 'x' * 350001
        expected_token = field
    elif kind == 'too-many-spaces':
        manifest.setdefault('dependsOn', {})['spaces'] = draw(st.lists(_dep_keys, min_size=11, max_size=13))
    elif kind == 'too-many-connectors':
        manifest.setdefault('dependsOn', {})['actionConnectors'] = draw(st.lists(_dep_keys, min_size=11, max_size=13))
    return manifest, expected_token


# Feature: cid-cmd-agent-flow-platform, Property 7: Cap validation rejects exactly the out-of-bounds fields, including persona length bounds and Space/connector count caps
@settings(max_examples=100, deadline=None)
@given(case=_cap_cases())
def test_property_7_cap_validation(case):
    """**Validates: Requirements 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7**"""
    manifest, expected_token = case
    if expected_token is None:
        # In-bounds manifest: must not raise
        assert validate_caps(manifest) is None
    else:
        # Out-of-bounds manifest: CidError naming the offending field
        with pytest.raises(CidError) as excinfo:
            validate_caps(manifest)
        assert expected_token in str(excinfo.value)


# ---------------------------------------------------------------------------
# Properties 10-12: dependency classification
# ---------------------------------------------------------------------------
@settings(max_examples=100, deadline=None)
@given(
    dashboards=st.lists(_dep_keys.map(lambda s: 'dep-' + s), max_size=8),
    present_keys=st.frozensets(_dep_keys.map(lambda s: 'other-' + s), max_size=8),
)
def test_property_10_all_absent_yields_empty_present(dashboards, present_keys):
    """Nothing declared is deployed: present is empty and everything is missing."""
    result = classify_dependencies(dashboards, present_keys)
    assert result['present'] == []
    assert set(result['missing']) == set(dashboards)


@st.composite
def _classification_cases(draw):
    dashboards = draw(st.lists(_dep_keys, max_size=8))
    declared = sorted(set(dashboards))
    present = draw(st.frozensets(st.sampled_from(declared))) if declared else frozenset()
    # extra present ids never declared as dependencies must be ignored
    extra = draw(st.frozensets(_dep_keys.map(lambda s: 'extra-' + s), max_size=4))
    return dashboards, present, extra


@settings(max_examples=100, deadline=None)
@given(case=_classification_cases())
def test_property_11_present_and_missing_partition_the_declared_dashboards(case):
    """present + missing is exactly the declared set, each once, in declaration order."""
    dashboards, present, extra = case
    result = classify_dependencies(dashboards, present | extra)
    assert set(result['present']) == present & set(dashboards)
    assert set(result['missing']) == set(dashboards) - present
    assert set(result['present']).isdisjoint(result['missing'])
    unique_order = list(dict.fromkeys(dashboards))
    assert result['present'] == [k for k in unique_order if k in present]
    assert result['missing'] == [k for k in unique_order if k not in present]

# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 14: Persona drift ignores the
# server-derived summary
# Validates: Requirements 7.3
# ---------------------------------------------------------------------------
_persona_values = st.tuples(*(st.text(min_size=1, max_size=30) for _ in range(5)))


# Feature: cid-cmd-agent-flow-platform, Property 14: Persona drift ignores the server-derived summary
@settings(max_examples=100, deadline=None)
@given(
    desired_values=_persona_values,
    same=st.booleans(),
    deployed_values=_persona_values,
    desired_summary=st.text(max_size=30),
    deployed_summary=st.text(max_size=30),
)
def test_property_14_persona_drift_ignores_summary(desired_values, same, deployed_values, desired_summary, deployed_summary):
    """**Validates: Requirements 7.3**"""
    if same:
        deployed_values = desired_values
    # desired uses the catalog camelCase keys + server-derived summary noise
    desired = dict(zip(PERSONA_FIELDS, desired_values))
    desired['promptSummary'] = desired_summary
    # deployed uses the API PascalCase read shape + its own summary noise
    deployed = dict(zip(PERSONA_API_FIELDS, deployed_values))
    deployed['PromptSummary'] = deployed_summary
    # drift iff at least one of the five fields differs; summaries never matter
    assert persona_differs(desired, deployed) == (desired_values != deployed_values)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 15: Association updates are a
# correct add/remove delta
# Validates: Requirements 7.4
# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 15: Association updates are a correct add/remove delta
@settings(max_examples=100, deadline=None)
@given(desired=_arn_sets, current=_arn_sets)
def test_property_15_association_delta(desired, current):
    """**Validates: Requirements 7.4**"""
    to_add, to_remove = compute_space_delta(desired, current)
    assert to_add == set(desired) - set(current)
    assert to_remove == set(current) - set(desired)
    # applying the delta to current yields exactly desired
    assert (set(current) - to_remove) | to_add == set(desired)
    # the delta never overlaps
    assert to_add.isdisjoint(to_remove)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 8: Space resource updates are
# additive and de-duplicated
# Validates: Requirements 6.7, 9.2, 9.4
# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 8: Space resource updates are additive and de-duplicated
@settings(max_examples=100, deadline=None)
@given(
    current=_arn_sets,
    desired_with_dups=st.lists(_arns, max_size=15),
    agent_desires=st.lists(st.lists(_arns, max_size=6), max_size=5),
)
def test_property_8_additive_deduplicated_updates(current, desired_with_dups, agent_desires):
    """**Validates: Requirements 6.7, 9.2, 9.4**"""
    additions = compute_space_additions(current, desired_with_dups)
    # AddResources equals desired - current: nothing already present is re-added
    assert additions == set(desired_with_dups) - set(current)
    assert additions.isdisjoint(current)
    # a set return guarantees de-duplication even when desired repeats ARNs
    assert isinstance(additions, set)
    # repeated adds from multiple agents yield the union, each ARN present once
    space = set(current)
    for desired in agent_desires:
        space |= compute_space_additions(space, desired)
    expected_union = set(current)
    for desired in agent_desires:
        expected_union |= set(desired)
    assert space == expected_union


# ---------------------------------------------------------------------------
# Property 9: --on-drift override removes exactly the catalog-managed resource types
# that the Space config no longer needs, and never touches other types
_resource_types = st.sampled_from(list(SPACE_MANAGED_RESOURCE_TYPES) + ['TOPIC', 'ACTION_CONNECTOR'])


@settings(max_examples=100, deadline=None)
@given(
    current=st.dictionaries(st.text(min_size=1, max_size=8), _resource_types, max_size=10),
    desired_arns=_arn_sets,
)
def test_property_9_override_space_removals(current, desired_arns):
    removals = compute_space_removals(current, desired_arns)
    # exactly the managed-type resources in the Space that the config does not need
    assert removals == {arn: kind for arn, kind in current.items()
                        if kind in SPACE_MANAGED_RESOURCE_TYPES and arn not in set(desired_arns)}
    # never a desired resource, never an unmanaged type, never something not in the Space
    assert set(removals).isdisjoint(desired_arns)
    assert all(kind in SPACE_MANAGED_RESOURCE_TYPES for kind in removals.values())
    assert set(removals) <= set(current)


# ---------------------------------------------------------------------------
# Property 19: the listing keeps catalog order, lists every entry once and marks
# exactly the deployed ones
# ---------------------------------------------------------------------------
_catalog_keys = st.text(alphabet='abcdefghijklmnopqrstuvwxyz-', min_size=1, max_size=10)


@st.composite
def _listing_cases(draw):
    keys = draw(st.lists(_catalog_keys, max_size=8, unique=True))
    catalog = {}
    for key in keys:
        entry = {}
        if draw(st.booleans()):
            entry['agentId'] = draw(_catalog_keys)
        if draw(st.booleans()):
            entry['name'] = draw(st.text(min_size=1, max_size=15))
        catalog[key] = entry
    # deployed ids drawn from catalog keys, agent ids, and unrelated ids
    candidates = list(keys) + [e.get('agentId') for e in catalog.values() if e.get('agentId')] + ['unrelated-id']
    deployed = draw(st.frozensets(st.sampled_from(candidates))) if candidates else frozenset()
    return catalog, deployed


@settings(max_examples=100, deadline=None)
@given(case=_listing_cases())
def test_property_19_listing_keeps_order_and_marks_deployed(case):
    catalog, deployed = case
    listing = build_agent_listing(catalog, deployed)
    assert [item['key'] for item in listing] == list(catalog)   # catalog order, each entry once
    for item in listing:
        entry = catalog[item['key']]
        agent_id = entry.get('agentId') or item['key']
        assert item['agentId'] == agent_id
        expected_deployed = item['key'] in deployed or agent_id in deployed
        assert item['deployed'] == expected_deployed
        assert (DEPLOYED_MARK in item['display']) == expected_deployed


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 27: CID-managed detection recognizes
# either the provenance tag or the Description marker
# Validates: Requirements 13.4, 12.7, 13.1, 13.2
# ---------------------------------------------------------------------------
_tag_keys = st.text(min_size=1, max_size=15).filter(lambda s: s != CID_PROVENANCE_TAG_KEY)
_descriptions = st.one_of(st.none(), st.text(max_size=60).filter(lambda s: CID_MANAGED_MARKER not in s))


@st.composite
def _cid_managed_cases(draw):
    has_tag = draw(st.booleans())
    has_marker = draw(st.booleans())
    tag_shape = draw(st.sampled_from(['dict', 'list', 'none']))
    other_tags = draw(st.dictionaries(_tag_keys, st.text(max_size=10), max_size=4))
    if has_tag:
        other_tags[CID_PROVENANCE_TAG_KEY] = draw(st.text(max_size=10))
    if tag_shape == 'dict':
        tags = dict(other_tags)
        effective_tag = has_tag
    elif tag_shape == 'list':
        tags = [{'Key': k, 'Value': v} for k, v in other_tags.items()]
        effective_tag = has_tag
    else:
        tags = None
        effective_tag = False
    description = draw(_descriptions)
    if has_marker:
        prefix = draw(st.text(max_size=15).filter(lambda s: CID_MANAGED_MARKER not in s + CID_MANAGED_MARKER[:len(s)]))
        description = f'{prefix}{CID_MANAGED_MARKER}{description or ""}'
    return tags, description, effective_tag, has_marker


# Feature: cid-cmd-agent-flow-platform, Property 27: CID-managed detection recognizes either the provenance tag or the Description marker
@settings(max_examples=100, deadline=None)
@given(case=_cid_managed_cases())
def test_property_27_dual_mechanism_cid_managed(case):
    """**Validates: Requirements 13.4, 12.7, 13.1, 13.2**"""
    tags, description, has_tag, has_marker = case
    # true iff the provenance tag is present OR the description carries the marker;
    # absence of both returns false
    assert is_cid_managed(tags, description) == (has_tag or has_marker)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 28: Space-id derivation is
# deterministic and produces pattern-valid ids
# Validates: Requirements 9.7
# ---------------------------------------------------------------------------
_SPACE_ID_PATTERN = re.compile(r'[0-9a-zA-Z\-_=.+]+')


# Feature: cid-cmd-agent-flow-platform, Property 28: Space-id derivation is deterministic and produces pattern-valid ids
@settings(max_examples=100, deadline=None)
@given(name=st.one_of(st.text(max_size=40), st.none()))
def test_property_28_space_id_derivation(name):
    """**Validates: Requirements 9.7**"""
    first = derive_space_id(name)
    second = derive_space_id(name)
    # deterministic: the same input always yields the same output
    assert first == second
    # non-empty and pattern-valid
    assert first
    assert _SPACE_ID_PATTERN.fullmatch(first), f'derived id {first!r} violates the SpaceId pattern'
