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
    HIDDEN_CATEGORY,
    PARTITION_DOMAINS,
    PERSONA_API_FIELDS,
    PERSONA_FIELDS,
    build_agent_listing,
    build_console_url,
    classify_dependencies,
    compute_space_additions,
    compute_space_delta,
    compute_stale_removals,
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
    resource_kind=st.sampled_from(['dashboard', 'agent', 'space', 'dashboards', 'agents', 'spaces', 'Agent ', 'SPACE']),
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
    # gen-AI resources are account-scoped
    kind = resource_kind.strip().lower().rstrip('s')
    if kind in ('agent', 'space'):
        assert account_id in url
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
# Feature: cid-cmd-agent-flow-platform, Property 10: Zero dashboards present raises a
# guidance error and creates nothing (classification portion: all-absent yields an
# empty present set)
# Validates: Requirements 8.3, 19.1
# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 10: Zero dashboards present raises a guidance error and creates nothing
@settings(max_examples=100, deadline=None)
@given(
    required=st.lists(_dep_keys.map(lambda s: 'dep-' + s), max_size=8),
    optional=st.lists(_dep_keys.map(lambda s: 'dep-' + s), max_size=8),
    present_keys=st.frozensets(_dep_keys.map(lambda s: 'other-' + s), max_size=8),
)
def test_property_10_all_absent_yields_empty_present(required, optional, present_keys):
    """**Validates: Requirements 8.3, 19.1**"""
    # present_keys is disjoint from every declared dependency ('other-' vs 'dep-' prefixes)
    result = classify_dependencies(required, optional, present_keys)
    assert result['present'] == []
    # everything declared is reported missing
    assert set(result['missing_required']) | set(result['missing_optional']) == set(required) | set(optional)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 11: At least one present dashboard
# proceeds using exactly the present set
# Validates: Requirements 8.4, 19.2, 19.4
# ---------------------------------------------------------------------------
@st.composite
def _partitioned_deps(draw):
    required = draw(st.lists(_dep_keys, max_size=8))
    optional = draw(st.lists(_dep_keys, max_size=8))
    declared = sorted(set(required) | set(optional))
    if declared:
        present = draw(st.frozensets(st.sampled_from(declared), min_size=1))
    else:
        present = frozenset()
    # extra present keys never declared as dependencies must be ignored
    extra = draw(st.frozensets(_dep_keys.map(lambda s: 'extra-' + s), max_size=4))
    return required, optional, present, extra


# Feature: cid-cmd-agent-flow-platform, Property 11: At least one present dashboard proceeds using exactly the present set
@settings(max_examples=100, deadline=None)
@given(case=_partitioned_deps())
def test_property_11_proceeds_with_exactly_present_set(case):
    """**Validates: Requirements 8.4, 19.2, 19.4**"""
    required, optional, present, extra = case
    result = classify_dependencies(required, optional, present | extra)
    declared = set(required) | set(optional)
    # exactly the present declared dependencies, never an absent or undeclared one
    assert set(result['present']) == present & declared
    if present & declared:
        assert result['present'], 'at least one present dashboard must let the run proceed'
    # each key appears at most once
    assert len(result['present']) == len(set(result['present']))
    # present preserves declaration order (required first, then optional)
    declaration_order = [k for k in list(required) + list(optional)]
    filtered = [k for i, k in enumerate(declaration_order) if k in set(result['present']) and k not in declaration_order[:i]]
    assert result['present'] == filtered


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 12: Missing required vs missing
# optional emit the correct guidance and never deploy (classification portion:
# correct required/optional partitioning)
# Validates: Requirements 8.5, 8.6, 19.3, 19.5
# ---------------------------------------------------------------------------
@st.composite
def _missing_split_cases(draw):
    # 'req-'/'opt-' prefixes keep the two dependency namespaces disjoint
    required = draw(st.lists(_dep_keys.map(lambda s: 'req-' + s), max_size=8))
    optional = draw(st.lists(_dep_keys.map(lambda s: 'opt-' + s), max_size=8))
    declared = sorted(set(required) | set(optional))
    present = set()
    if declared:
        present |= draw(st.frozensets(st.sampled_from(declared)))
    # undeclared present keys must not disturb the split
    present |= draw(st.frozensets(_dep_keys.map(lambda s: 'extra-' + s), max_size=4))
    return required, optional, frozenset(present)


# Feature: cid-cmd-agent-flow-platform, Property 12: Missing required vs missing optional emit the correct guidance and never deploy
@settings(max_examples=100, deadline=None)
@given(case=_missing_split_cases())
def test_property_12_missing_required_vs_optional_partitioning(case):
    """**Validates: Requirements 8.5, 8.6, 19.3, 19.5**"""
    required, optional, present_keys = case
    result = classify_dependencies(required, optional, present_keys)
    present_set = set(present_keys)

    def _dedup(seq):
        seen = set()
        out = []
        for key in seq:
            if key not in seen:
                seen.add(key)
                out.append(key)
        return out

    # missing_required is exactly the absent required keys, in declaration order, deduped
    assert result['missing_required'] == [k for k in _dedup(required) if k not in present_set]
    # missing_optional is exactly the absent optional keys, in declaration order, deduped
    assert result['missing_optional'] == [k for k in _dedup(optional) if k not in present_set]
    # required and optional missing sets are disjoint ('req-'/'opt-' prefixes) and
    # together with present partition the declared dependency set
    assert set(result['missing_required']).isdisjoint(result['missing_optional'])
    assert set(result['present']) | set(result['missing_required']) | set(result['missing_optional']) == set(required) | set(optional)
    assert set(result['present']).isdisjoint(result['missing_required'])
    assert set(result['present']).isdisjoint(result['missing_optional'])


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
# Feature: cid-cmd-agent-flow-platform, Property 9: Stale removal is opt-in and
# scoped to CID-managed, unreferenced resources
# Validates: Requirements 9.5, 12.6
# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 9: Stale removal is opt-in and scoped to CID-managed, unreferenced resources
@settings(max_examples=100, deadline=None)
@given(
    space_arns=_arn_sets,
    referenced_arns=_arn_sets,
    managed_arns=_arn_sets,
    remove_stale=st.booleans(),
)
def test_property_9_scoped_stale_removal(space_arns, referenced_arns, managed_arns, remove_stale):
    """**Validates: Requirements 9.5, 12.6**"""
    removals = compute_stale_removals(space_arns, referenced_arns, managed_arns, remove_stale)
    if not remove_stale:
        # opt-in: flag off removes nothing
        assert removals == set()
    else:
        # exactly the CID-managed, unreferenced resources actually in the Space
        assert removals == (set(space_arns) & set(managed_arns)) - set(referenced_arns)
        # never a non-CID resource
        assert removals <= set(managed_arns)
        # never a still-referenced resource
        assert removals.isdisjoint(referenced_arns)
        # never something not in the Space
        assert removals <= set(space_arns)


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 19: Listing and pickers group by
# category, mark deployed, and hide deprecated
# Validates: Requirements 6.2, 11.1, 11.2, 11.4
# ---------------------------------------------------------------------------
_catalog_keys = st.text(alphabet='abcdefghijklmnopqrstuvwxyz-', min_size=1, max_size=10)


@st.composite
def _listing_cases(draw):
    keys = draw(st.lists(_catalog_keys, max_size=8, unique=True))
    catalog = {}
    for key in keys:
        entry = {}
        category = draw(st.sampled_from([None, 'Cost', 'Operations', 'Security', HIDDEN_CATEGORY]))
        if category is not None:
            entry['category'] = category
        if draw(st.booleans()):
            entry['agentId'] = draw(_catalog_keys)
        if draw(st.booleans()):
            entry['name'] = draw(st.text(min_size=1, max_size=15))
        catalog[key] = entry
    # deployed ids drawn from catalog keys, agent ids, and unrelated ids
    candidates = list(keys) + [e.get('agentId') for e in catalog.values() if e.get('agentId')] + ['unrelated-id']
    deployed = draw(st.frozensets(st.sampled_from(candidates))) if candidates else frozenset()
    return catalog, deployed


# Feature: cid-cmd-agent-flow-platform, Property 19: Listing and pickers group by category, mark deployed, and hide deprecated
@settings(max_examples=100, deadline=None)
@given(case=_listing_cases())
def test_property_19_listing_groups_marks_hides(case):
    """**Validates: Requirements 6.2, 11.1, 11.2, 11.4**"""
    catalog, deployed = case
    listing = build_agent_listing(catalog, deployed)
    # zero entries in category Deprecated
    assert HIDDEN_CATEGORY not in listing
    # flatten and count: every non-deprecated entry appears exactly once, under its category
    seen_keys = []
    for category, entries in listing.items():
        for item in entries:
            seen_keys.append(item['key'])
            entry = catalog[item['key']]
            assert str(entry.get('category') or 'Other') == category
            agent_id = entry.get('agentId') or item['key']
            assert item['agentId'] == agent_id
            # check indicator marks exactly the deployed entries
            expected_deployed = item['key'] in deployed or agent_id in deployed
            assert item['deployed'] == expected_deployed
            assert (DEPLOYED_MARK in item['display']) == expected_deployed
    visible = [k for k, e in catalog.items() if (e.get('category') or 'Other') != HIDDEN_CATEGORY]
    assert sorted(seen_keys) == sorted(visible)
    assert len(seen_keys) == len(set(seen_keys))


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


# ---------------------------------------------------------------------------
# Feature: cid-cmd-agent-flow-platform, Property 29: Agent parameter-store database
# selection is deterministic and majority-based
# Validates: Requirements 20.7
# ---------------------------------------------------------------------------
from collections import Counter

from cid.helpers.quicksight.agent_logic import select_database_from_candidates

_db_names = st.text(alphabet='abcdefgh_', min_size=1, max_size=6)


@st.composite
def _database_candidate_lists(draw):
    """Lists of database names with deliberate duplicates and ties.

    Half the time the list is built from explicit (name, count) pairs — repeated
    names guarantee duplicates and the small alphabet makes count ties frequent —
    then shuffled so order never influences the selection. The rest of the time a
    plain (possibly empty) list is drawn.
    """
    if draw(st.booleans()):
        pairs = draw(st.lists(st.tuples(_db_names, st.integers(min_value=1, max_value=4)), max_size=6))
        names = [name for name, count in pairs for _ in range(count)]
        return draw(st.permutations(names)) if names else names
    return draw(st.lists(_db_names, max_size=20))


# Feature: cid-cmd-agent-flow-platform, Property 29: Agent parameter-store database selection is deterministic and majority-based
@settings(max_examples=100, deadline=None)
@given(candidates=st.one_of(st.none(), _database_candidate_lists()))
def test_property_29_database_selection_deterministic_majority(candidates):
    """**Validates: Requirements 20.7**"""
    first = select_database_from_candidates(candidates)
    # calling twice with the same input yields the same result (determinism)
    second = select_database_from_candidates(candidates)
    assert first == second
    if not candidates:
        # empty list or None -> None
        assert first is None
    else:
        counts = Counter(candidates)
        max_count = max(counts.values())
        # the returned name has the maximal count ...
        assert counts[first] == max_count
        # ... and among all maximal-count names it is the lexicographically smallest
        winners = sorted(name for name, count in counts.items() if count == max_count)
        assert first == winners[0]


# ======================================================================
# describe_drift
# ======================================================================

from cid.helpers.quicksight.agent_logic import describe_drift


def _deployed_matching(definition):
    """DescribeAgent read shape exactly matching a catalog definition."""
    persona = {key[0].upper() + key[1:]: str(value)
               for key, value in (definition.get('persona') or {}).items()}
    return {
        'Name': definition.get('name'),
        'CustomPromptInterface': persona,
        'StarterPrompts': list(definition.get('starterPrompts') or []),
        'WelcomeMessage': definition.get('welcomeMessage'),
        'ActionConnectors': list((definition.get('dependsOn') or {}).get('actionConnectors') or []),
    }


def _drift_definition():
    return {
        'name': 'Agent',
        'persona': {'identity': 'id-1', 'customInstructions': 'ci-1', 'tone': 't-1',
                    'outputStyle': 'os-1', 'responseLength': 'rl-1'},
        'starterPrompts': ['p1'],
        'welcomeMessage': 'hello',
        'dependsOn': {'actionConnectors': []},
    }


def test_describe_drift_empty_when_matching():
    definition = _drift_definition()
    assert describe_drift(definition, _deployed_matching(definition)) == []


def test_describe_drift_names_the_drifted_persona_field():
    definition = _drift_definition()
    deployed = _deployed_matching(definition)
    deployed['CustomPromptInterface']['Tone'] = 'different'
    assert describe_drift(definition, deployed) == ['persona.Tone']


def test_describe_drift_reports_each_managed_field():
    definition = _drift_definition()
    deployed = _deployed_matching(definition)
    deployed['Name'] = 'Other'
    deployed['StarterPrompts'] = ['p2']
    deployed['WelcomeMessage'] = 'bye'
    deployed['ActionConnectors'] = ['arn:connector']
    assert describe_drift(definition, deployed) == ['name', 'starterPrompts', 'welcomeMessage', 'actionConnectors']


def test_describe_drift_ignores_unmanaged_fields():
    """Fields the definition does not carry (None) never count as drift."""
    definition = {'name': None, 'persona': {}, 'starterPrompts': None, 'welcomeMessage': None}
    deployed = {'Name': 'Anything', 'StarterPrompts': ['x'], 'WelcomeMessage': 'y',
                'CustomPromptInterface': {'Tone': 'z'}}
    assert describe_drift(definition, deployed) == []
