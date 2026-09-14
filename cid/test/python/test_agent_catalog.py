""" Tests for agent catalog loading, launch-library content completeness, and get_definition.
"""

import tempfile
from pathlib import Path
import pytest
import yaml
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
    """Create <root>/agents/catalog.yaml + agents/<id>/<id>.yaml + spaces/<id>.yaml.

    Mirrors the real repo convention: agents have their own catalog next to
    the agent folders, catalog urls are relative to the catalog file, and
    agent manifests are kind-wrapped in per-agent folders.
    """
    root = Path(root)
    (root / 'agents').mkdir(parents=True, exist_ok=True)
    urls = []
    for agent_id, entry in (agents or {}).items():
        agent_dir = root / 'agents' / agent_id
        agent_dir.mkdir(parents=True)
        (agent_dir / f'{agent_id}.yaml').write_text(yaml.safe_dump({'agents': {agent_id: entry}}))
        urls.append(f'{agent_id}/{agent_id}.yaml')
    for space_id, entry in (spaces or {}).items():
        spaces_dir = root / 'spaces'
        spaces_dir.mkdir(parents=True, exist_ok=True)
        (spaces_dir / f'{space_id}.yaml').write_text(yaml.safe_dump({'spaces': {space_id: entry}}))
        urls.append(f'../spaces/{space_id}.yaml')
    catalog = root / 'agents' / 'catalog.yaml'
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
        assert Path(loaded_agent['source']).resolve() == (root / 'agents' / 'my-agent' / 'my-agent.yaml').resolve()
        assert Path(loaded_space['source']).resolve() == (root / 'spaces' / 'my-space.yaml').resolve()


def test_persona_file_resolves_against_manifest_folder():
    """A relative personaFile resolves against the manifest's source folder and
    returns the parsed persona mapping."""
    persona = {'identity': 'intended sibling content'}
    with tempfile.TemporaryDirectory(prefix='cid_catalog_test_') as tmpdir:
        catalog = _write_repo_layout(
            tmpdir, agents={'my-agent': {'name': 'agent', 'personaFile': 'persona.yaml'}})
        (Path(tmpdir) / 'agents' / 'my-agent' / 'persona.yaml').write_text(yaml.safe_dump(persona))

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
        bad = Path(tmpdir) / 'agents' / 'bad'
        bad.mkdir(parents=True)
        (bad / 'bad.yaml').write_text('agents: [unclosed\n  sequence: {')
        catalog_content = yaml.safe_load(catalog.read_text())
        catalog_content['Resources'].insert(0, {'Url': 'bad/bad.yaml'})
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
    """agents/catalog.yaml references the agent and shared space files, and
    every listed url resolves to a file."""
    catalog = yaml.safe_load((REPO_ROOT / 'agents' / 'catalog.yaml').read_text())
    urls = [resource.get('Url') for resource in catalog.get('Resources', [])]
    for expected in ('finops/finops.yaml',
                     'operations/operations.yaml',
                     'security/security.yaml',
                     '../spaces/cid-dashboards-space.yaml'):
        assert expected in urls, f'{expected} is not listed in agents/catalog.yaml'
    for url in urls:
        referenced = (REPO_ROOT / 'agents' / url).resolve()
        assert referenced.is_file(), f'catalog url {url} does not resolve to a file'


def test_dashboards_catalog_carries_no_agent_entries():
    """dashboards/catalog.yaml stays dashboards-only; agents have their own catalog."""
    catalog = yaml.safe_load((REPO_ROOT / 'dashboards' / 'catalog.yaml').read_text())
    urls = [resource.get('Url') or '' for resource in catalog.get('Resources', [])]
    assert not [url for url in urls if 'agents/' in url or 'spaces/' in url]


def test_default_catalog_urls_include_the_agents_catalog():
    """The Cid default catalog list carries both the dashboards and the agents
    catalogs, so agents load without extra flags once merged upstream."""
    cid_obj = Cid()
    assert any(url.endswith('dashboards/catalog.yaml') for url in cid_obj.catalog_urls)
    assert any(url.endswith('agents/catalog.yaml') for url in cid_obj.catalog_urls)


# ======================================================================
# from test_launch_content.py
# ======================================================================


DASHBOARDS_CATALOG_PATH = REPO_ROOT / 'dashboards' / 'catalog.yaml'
AGENTS_CATALOG_PATH = REPO_ROOT / 'agents' / 'catalog.yaml'


LAUNCH_AGENT_IDS = ('operations', 'finops', 'security')


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
    """Every dependsOn.dashboards / optionalDashboards key resolves to a
    dashboard catalog entry, every dependsOn.datasets key resolves to a
    datasets catalog entry, and every dependsOn.spaces id resolves to the
    spaces catalog."""
    agent = RESOURCES['agents'][agent_id]
    depends_on = agent.get('dependsOn', {})

    dashboards_catalog = RESOURCES.get('dashboards', {})
    datasets_catalog = RESOURCES.get('datasets', {})
    spaces_catalog = RESOURCES.get('spaces', {})

    for key in list(depends_on.get('dashboards', [])) + list(depends_on.get('optionalDashboards', [])):
        assert key in dashboards_catalog, (
            f'Agent {agent_id!r} references dashboard key {key!r} '
            f'that is not in the dashboards catalog')

    for key in depends_on.get('datasets', []):
        assert key in datasets_catalog, (
            f'Agent {agent_id!r} references dataset key {key!r} '
            f'that is not in the datasets catalog')

    for space_id in depends_on.get('spaces', []):
        assert space_id in spaces_catalog, (
            f'Agent {agent_id!r} references space id {space_id!r} '
            f'that is not in the spaces catalog')


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
    assert 'cid-dashboards-space' in RESOURCES.get('spaces', {})


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
