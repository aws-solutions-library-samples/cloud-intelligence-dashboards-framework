""" Tests for agent catalog loading, launch-library content completeness, and get_definition.
"""

import tempfile
from pathlib import Path
import pytest
import yaml
import cid.utils as cid_utils
from cid.common import Cid
from cid.exceptions import CidCritical, CidError
import os
from cid.plugin import Plugin
from hypothesis import given, settings, strategies as st


# ======================================================================
# from test_catalog_loading.py
# ======================================================================


REPO_ROOT = Path(__file__).resolve().parents[3]


def _make_catalog_cid():
    """Build a Cid with only the attributes the resource loaders touch."""
    cid_obj = Cid.__new__(Cid)
    cid_obj.resources = {}
    return cid_obj


def _write_repo_layout(root, agents=None, spaces=None):
    """Create <root>/agentic/catalog.yaml + agentic/agents/<id>/<id>.yaml + agentic/spaces/<id>.yaml.

    Mirrors the real repo convention: one agentic catalog with agents and Spaces in
    sub-folders, catalog urls relative to the catalog file, kind-wrapped manifests.
    """
    base = Path(root) / 'agentic'
    base.mkdir(parents=True, exist_ok=True)
    urls = []
    for agent_id, entry in (agents or {}).items():
        agent_dir = base / 'agents' / agent_id
        agent_dir.mkdir(parents=True)
        (agent_dir / f'{agent_id}.yaml').write_text(yaml.safe_dump({'agents': {agent_id: entry}}))
        urls.append(f'agents/{agent_id}/{agent_id}.yaml')
    for space_id, entry in (spaces or {}).items():
        spaces_dir = base / 'spaces'
        spaces_dir.mkdir(parents=True, exist_ok=True)
        (spaces_dir / f'{space_id}.yaml').write_text(yaml.safe_dump({'spaces': {space_id: entry}}))
        urls.append(f'spaces/{space_id}.yaml')
    catalog = base / 'catalog.yaml'
    catalog.write_text(yaml.safe_dump({'Resources': [{'Url': url} for url in urls]}))
    return catalog


def test_catalog_registers_agents_and_spaces_with_source():
    """Kind-wrapped agent and space files listed in the catalog merge into
    resources['agents']/['spaces'] with content preserved and source metadata
    pointing at the originating file — through the same pipeline dashboards use."""
    agents = {'my-agent': {'name': 'my agent', 'category': 'FinOps'}}
    spaces = {'my-space': {'name': 'my space'}}
    with tempfile.TemporaryDirectory(prefix='cid_catalog_test_') as tmpdir:
        catalog = _write_repo_layout(tmpdir, agents=agents, spaces=spaces)
        cid_obj = _make_catalog_cid()
        cid_obj.load_catalog(str(catalog))

        loaded_agent = cid_obj.resources['agents']['my-agent']
        loaded_space = cid_obj.resources['spaces']['my-space']
        assert loaded_agent['name'] == 'my agent'
        assert loaded_agent['category'] == 'FinOps'
        assert loaded_space['name'] == 'my space'
        root = Path(tmpdir)
        assert Path(loaded_agent['source']).resolve() == (root / 'agentic' / 'agents' / 'my-agent' / 'my-agent.yaml').resolve()
        assert Path(loaded_space['source']).resolve() == (root / 'agentic' / 'spaces' / 'my-space.yaml').resolve()


def test_persona_file_resolves_against_manifest_folder():
    """A relative personaFile resolves against the manifest's source folder and
    returns the parsed persona mapping."""
    persona = {'identity': 'intended sibling content'}
    with tempfile.TemporaryDirectory(prefix='cid_catalog_test_') as tmpdir:
        catalog = _write_repo_layout(
            tmpdir, agents={'my-agent': {'name': 'agent', 'personaFile': 'persona.yaml'}})
        (Path(tmpdir) / 'agentic' / 'agents' / 'my-agent' / 'persona.yaml').write_text(yaml.safe_dump(persona))

        cid_obj = _make_catalog_cid()
        cid_obj.load_catalog(str(catalog))
        definition = cid_obj.resources['agents']['my-agent']

        assert cid_obj._load_agent_persona(definition) == persona


def test_inline_persona_wins_over_persona_file():
    """An inline persona mapping is used as-is without touching personaFile."""
    cid_obj = _make_catalog_cid()
    persona = {'identity': 'inline'}
    assert cid_obj._load_agent_persona({'persona': persona, 'personaFile': 'x.yaml'}) == persona


def test_malformed_resource_file_is_skipped_and_loading_continues():
    """A malformed resource file is skipped with a warning; the remaining
    catalog entries still load (same behavior dashboards rely on)."""
    with tempfile.TemporaryDirectory(prefix='cid_catalog_test_') as tmpdir:
        catalog = _write_repo_layout(tmpdir, agents={'good': {'name': 'good agent'}})
        bad = Path(tmpdir) / 'agentic' / 'agents' / 'bad'
        bad.mkdir(parents=True)
        (bad / 'bad.yaml').write_text('agents: [unclosed\n  sequence: {')
        catalog_content = yaml.safe_load(catalog.read_text())
        catalog_content['Resources'].insert(0, {'Url': 'agents/bad/bad.yaml'})
        catalog.write_text(yaml.safe_dump(catalog_content))

        cid_obj = _make_catalog_cid()
        cid_obj.load_catalog(str(catalog))

        assert 'bad' not in cid_obj.resources.get('agents', {})
        assert 'good' in cid_obj.resources['agents']


def test_missing_persona_file_raises_naming_the_file():
    """A personaFile that does not resolve raises a CID error naming the file.

    A missing local file surfaces as CidCritical from resolve_relative_path
    (the tool's fatal-error pattern); a present-but-unreadable file surfaces
    as CidError from the persona wrapper. Both name the reference.
    """
    with tempfile.TemporaryDirectory(prefix='cid_catalog_test_') as tmpdir:
        catalog = _write_repo_layout(
            tmpdir, agents={'my-agent': {'name': 'agent', 'personaFile': 'missing.yaml'}})
        cid_obj = _make_catalog_cid()
        cid_obj.load_catalog(str(catalog))
        definition = cid_obj.resources['agents']['my-agent']

        with pytest.raises((CidError, CidCritical)) as excinfo:
            cid_obj._load_agent_persona(definition)
        assert 'missing.yaml' in str(excinfo.value)


def test_repo_agents_catalog_lists_launch_agents_and_shared_space():
    """agentic/catalog.yaml references the agent and shared space files, and
    every listed url resolves to a file."""
    catalog = yaml.safe_load((REPO_ROOT / 'agentic' / 'catalog.yaml').read_text())
    urls = [resource.get('Url') for resource in catalog.get('Resources', [])]
    for expected in ('agents/finops/finops.yaml',
                     'agents/operations/operations.yaml',
                     'agents/security/security.yaml',
                     'agents/resilience/resilience.yaml',
                     'agents/support/support.yaml',
                     'agents/health/health.yaml',
                     'spaces/cid-finops-space.yaml',
                     'spaces/cid-operations-space.yaml',
                     'spaces/cid-security-space.yaml',
                     'spaces/cid-resilience-space.yaml',
                     'spaces/cid-support-space.yaml',
                     'spaces/cid-health-space.yaml'):
        assert expected in urls, f'{expected} is not listed in agentic/catalog.yaml'
    for url in urls:
        referenced = (REPO_ROOT / 'agentic' / url).resolve()
        assert referenced.is_file(), f'catalog url {url} does not resolve to a file'


def test_dashboards_catalog_carries_no_agent_entries():
    """dashboards/catalog.yaml stays dashboards-only; agents have their own catalog."""
    catalog = yaml.safe_load((REPO_ROOT / 'dashboards' / 'catalog.yaml').read_text())
    urls = [resource.get('Url') or '' for resource in catalog.get('Resources', [])]
    assert not [url for url in urls if 'agents/' in url or 'spaces/' in url]


def test_dashboard_commands_never_load_the_agents_catalog():
    """The default catalog list (loaded by every command) is dashboards-only; the
    agents catalog is a separate list loaded by the agent commands only."""
    cid_obj = Cid()
    assert all(url.endswith('dashboards/catalog.yaml') for url in cid_obj.catalog_urls)
    assert all(url.endswith('agentic/catalog.yaml') for url in cid_obj.agentic_catalog_urls)


def test_agentic_catalog_loads_once():
    cid_obj = Cid.__new__(Cid)
    cid_obj.resources = {}
    cid_obj.agentic_catalog_urls = ['https://example.invalid/agentic/catalog.yaml']
    loaded = []
    cid_obj.load_catalog = loaded.append
    cid_obj.load_agentic_catalog()
    cid_obj.load_agentic_catalog()
    assert loaded == ['https://example.invalid/agentic/catalog.yaml']


def test_default_agents_catalog_skipped_when_agents_already_loaded():
    """Agents passed via --catalog/--resources: the default agents URL is not fetched."""
    cid_obj = Cid.__new__(Cid)
    cid_obj.resources = {'agents': {'finops': {}}}
    cid_obj.agentic_catalog_urls = ['https://example.invalid/agents/catalog.yaml']
    loaded = []
    cid_obj.load_catalog = loaded.append
    cid_obj.load_agentic_catalog()
    assert loaded == []


# ======================================================================
# from test_launch_content.py
# ======================================================================


DASHBOARDS_CATALOG_PATH = REPO_ROOT / 'dashboards' / 'catalog.yaml'
AGENTS_CATALOG_PATH = REPO_ROOT / 'agentic' / 'catalog.yaml'


LAUNCH_AGENT_IDS = ('operations', 'finops', 'security', 'resilience', 'support', 'health')


PERSONA_FIELDS = ('identity', 'customInstructions', 'tone', 'outputStyle', 'responseLength')


def _load_runtime_resources():
    """Builtin plugin resources merged with both local repo catalogs (once),
    mirroring the runtime default catalog list."""
    resources = Plugin('cid.builtin.core').provides()
    cid_obj = Cid.__new__(Cid)
    cid_obj.resources = resources
    cid_obj.load_catalog(str(DASHBOARDS_CATALOG_PATH))
    cid_obj.load_catalog(str(AGENTS_CATALOG_PATH))
    return cid_obj.resources


RESOURCES = _load_runtime_resources()


@pytest.mark.parametrize('agent_id', LAUNCH_AGENT_IDS)
def test_launch_agent_is_referentially_complete(agent_id):
    """The agent references exactly one Space from the spaces catalog and declares
    no knowledge itself (dashboards/datasets/knowledge bases belong to the Space)."""
    depends_on = RESOURCES['agents'][agent_id].get('dependsOn', {})
    spaces = depends_on.get('spaces', [])
    assert len(spaces) == 1, f'Agent {agent_id!r} must reference exactly one Space (got {spaces})'
    assert spaces[0] in RESOURCES.get('spaces', {}), (
        f'Agent {agent_id!r} references space id {spaces[0]!r} that is not in the spaces catalog')
    misplaced = set(depends_on) & {'dashboards', 'optionalDashboards', 'datasets', 'knowledgeBases'}
    assert not misplaced, f'Agent {agent_id!r} declares {sorted(misplaced)}; move them to its Space'


# CID dashboards deployed from their own resources file (not in dashboards/catalog.yaml):
# dashboard id -> dataset names (matched by name against the deployed datasets at runtime).
EXTERNAL_DASHBOARDS = {
    # AWS Config Resource Compliance Dashboard:
    # https://github.com/aws-samples/config-resource-compliance-dashboard (dashboard_template/cid-crcd.yaml)
    'cid-crcd': {
        'config_compliance', 'config_compliance_actual', 'config_compliance_conformance_pack',
        'config_compliance_resource', 'config_event', 'config_event_delivery', 'config_event_rule_evaluation',
        'config_inventory', 'config_inventory_ebs', 'config_inventory_ec2', 'config_inventory_lambda',
        'config_inventory_rds', 'config_inventory_s3',
    },
}


@pytest.mark.parametrize('space_id', sorted(RESOURCES.get('spaces', {})))
def test_launch_space_is_referentially_complete(space_id):
    """Every Space dashboard id is the dashboardId of a dashboard catalog entry (or a
    known external CID dashboard) and every dataset key resolves to a datasets catalog
    entry (or a dataset of a known external dashboard listed in the same Space)."""
    depends_on = RESOURCES['spaces'][space_id].get('dependsOn', {})
    assert 'optionalDashboards' not in depends_on, (
        f"Space {space_id!r}: list every dashboard under 'dashboards' (optionalDashboards is not supported)")
    dashboard_ids = {entry.get('dashboardId') for entry in RESOURCES.get('dashboards', {}).values()}
    external_datasets = set()
    for dashboard_id in depends_on.get('dashboards', []):
        if dashboard_id in EXTERNAL_DASHBOARDS:
            external_datasets |= EXTERNAL_DASHBOARDS[dashboard_id]
            continue
        assert dashboard_id in dashboard_ids, (
            f'Space {space_id!r} references dashboard id {dashboard_id!r} that is not in the dashboards catalog')
    for key in depends_on.get('datasets', []):
        assert key in RESOURCES.get('datasets', {}) or key in external_datasets, (
            f'Space {space_id!r} references dataset key {key!r} that is not in the datasets catalog')


QUICK_SPACE_MAX_RESOURCES_PER_TYPE = 20   # Amazon Quick: at most 20 resources of each type per Space


@pytest.mark.parametrize('space_id', sorted(RESOURCES.get('spaces', {})))
def test_launch_space_respects_the_quick_resource_limit(space_id):
    depends_on = RESOURCES['spaces'][space_id].get('dependsOn', {})
    for kind in ('dashboards', 'datasets', 'knowledgeBases'):
        count = len(depends_on.get(kind) or [])
        assert count <= QUICK_SPACE_MAX_RESOURCES_PER_TYPE, (
            f'Space {space_id!r} lists {count} {kind}; Quick allows at most '
            f'{QUICK_SPACE_MAX_RESOURCES_PER_TYPE} per resource type')


def test_every_launch_space_is_used_by_an_agent():
    used = {space for agent in RESOURCES.get('agents', {}).values()
            for space in (agent.get('dependsOn') or {}).get('spaces', [])}
    assert set(RESOURCES.get('spaces', {})) <= used, 'Some Spaces are not referenced by any agent'


@pytest.mark.parametrize('agent_id', LAUNCH_AGENT_IDS)
def test_launch_agent_has_a_short_summary(agent_id):
    """The one-line summary shown in the picker and list-agents."""
    summary = RESOURCES['agents'][agent_id].get('summary')
    assert summary and '\n' not in summary and len(summary) <= 70, (
        f'Agent {agent_id!r} needs a one-line summary of at most 70 characters (got {summary!r})')


@pytest.mark.parametrize('agent_id', LAUNCH_AGENT_IDS)
def test_launch_agent_persona_is_complete(agent_id):
    """The personaFile resolves against the manifest's source folder and
    carries all five persona fields non-empty."""
    agent = RESOURCES['agents'][agent_id]
    persona_file = agent.get('personaFile')
    source = agent.get('source')
    assert persona_file and source, f'Agent {agent_id!r} has no personaFile reference'
    persona_path = os.path.join(os.path.dirname(source), persona_file)
    assert os.path.isfile(persona_path), (
        f'Agent {agent_id!r} personaFile does not resolve to a file: {persona_path}')
    with open(persona_path, encoding='utf-8') as handle:
        persona = yaml.safe_load(handle)
    assert isinstance(persona, dict), f'Agent {agent_id!r} persona is not a YAML mapping'
    for field in PERSONA_FIELDS:
        value = persona.get(field)
        assert isinstance(value, str) and value.strip(), (
            f'Agent {agent_id!r} persona field {field!r} is missing or empty')


def test_launch_agents_and_shared_space_register_via_catalog():
    """The three launch agents and the shared space register through the
    catalog pipeline."""
    for agent_id in LAUNCH_AGENT_IDS:
        assert agent_id in RESOURCES.get('agents', {}), f'Launch agent {agent_id!r} did not register'
    assert {'cid-finops-space', 'cid-operations-space'} <= set(RESOURCES.get('spaces', {}))


def test_builtin_plugin_ships_no_agents_or_spaces():
    """The builtin plugin data carries no agents or spaces kinds — they are
    catalog content, not package content."""
    builtin = Plugin('cid.builtin.core').provides()
    assert 'agents' not in builtin
    assert 'spaces' not in builtin


# ======================================================================
# from test_get_definition.py
# ======================================================================


ACCEPTED_TYPES = ('dashboard', 'dataset', 'view', 'schedule', 'crawler', 'agent', 'space')


KNOWN_PARAMS = {'kvar': 'resolved-value', 'kother': 'other-value'}


def _make_definition_cid(resources=None, params=None):
    """Build a Cid without __init__ and stub get_template_parameters."""
    cid_obj = Cid.__new__(Cid)
    base = {f'{t}s': {} for t in ACCEPTED_TYPES}
    base.update(resources or {})
    cid_obj.resources = base
    fixed = dict(KNOWN_PARAMS if params is None else params)
    cid_obj.get_template_parameters = lambda parameters, param_prefix='', others=None: dict(fixed)
    return cid_obj


_invalid_types = st.text(max_size=20).filter(lambda s: s not in ACCEPTED_TYPES)


@settings(max_examples=100, deadline=None)
@given(bad_type=_invalid_types)
def test_property_5_invalid_definition_types_are_rejected(bad_type):
    """Any type string outside the extended allowlist raises ValueError naming the type."""
    cid_obj = _make_definition_cid()
    with pytest.raises(ValueError) as excinfo:
        cid_obj.get_definition(bad_type, name='anything')
    assert 'is not a valid definition type' in str(excinfo.value)


@pytest.mark.parametrize('accepted_type', ACCEPTED_TYPES)
def test_property_5_accepted_types_are_not_rejected(accepted_type):
    """All 7 allowlisted types pass the type check (unknown name may return None)."""
    cid_obj = _make_definition_cid()
    result = cid_obj.get_definition(accepted_type, name='no-such-entry')
    assert result is None  # unknown name, but no unsupported-type ValueError


@pytest.mark.parametrize('definition_type,resource_key', [
    ('agent', 'agents'),
    ('space', 'spaces'),
])
def test_known_tokens_are_substituted(definition_type, resource_key):
    """: ${var} tokens with matching parameters are replaced in agent/space definitions."""
    cid_obj = _make_definition_cid(resources={resource_key: {
        'my-entry': {
            'name': 'my-entry',
            'description': 'value is ${kvar} and ${kother}',
            'parameters': {},
        },
    }})
    res = cid_obj.get_definition(definition_type, name='my-entry')
    assert res is not None
    assert res['description'] == 'value is resolved-value and other-value'


@pytest.mark.parametrize('definition_type,resource_key', [
    ('agent', 'agents'),
    ('space', 'spaces'),
])
def test_unresolved_token_raises_valueerror_naming_token(definition_type, resource_key):
    """: an unresolved ${var} token in an agent/space definition raises ValueError naming it."""
    cid_obj = _make_definition_cid(resources={resource_key: {
        'my-entry': {
            'name': 'my-entry',
            'description': 'known ${kvar} unknown ${missing_token}',
            'parameters': {},
        },
    }})
    with pytest.raises(ValueError) as excinfo:
        cid_obj.get_definition(definition_type, name='my-entry')
    assert 'missing_token' in str(excinfo.value)
