"""Mock-based unit tests for the agent-command Parameter_Store database inference.

Feature: cid-cmd-agent-flow-platform (task 18.4)

Under test: ``Cid._agent_parameter_store_ready`` / ``_resolve_agent_parameter_store`` /
``_infer_agent_athena_database`` and the guarded ``_load_agent_default_parameters`` /
``_dump_agent_default_parameters`` in ``cid/common.py``.

Harness: mirrors ``test_create_agent_flow.py`` / ``test_delete_agent_flow.py`` —
``Cid.__new__(Cid)`` with MagicMock helpers (``athena``, ``qs``,
``parameters_controller``) written into ``__dict__`` to preempt the
``cached_property`` descriptors, and the real ``cid.utils`` parameter state
isolated (save/restore) around every test. No real AWS call is ever made, and an
autouse fixture fails any test in which an interactive prompt would be displayed
during resolution.
"""
import inspect
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import cid.utils as cid_utils
import cid.helpers.athena as athena_module
from cid.common import Cid

ACCOUNT_ID = '123456789012'
REGION = 'us-east-1'
CATALOG = 'AwsDataCatalog'
DEFAULT_WORKGROUP = 'CID'  # Athena.defaults['WorkGroup']
AGENT_KEY = 'test-agent'
SKIP_LOG_FRAGMENT = 'Skipping the agent parameter store'
INFERENCE_LOG_FRAGMENT = 'Inferred the Athena database'

USABLE_WORKGROUP = {'WorkGroup': {
    'State': 'ENABLED',
    'Configuration': {'ResultConfiguration': {'OutputLocation': 's3://bucket/results/'}},
}}


def reset_parameters(initial=None):
    """Reset the cid.utils global parameter state (params/defaults/all_yes)."""
    cid_utils.params.clear()
    cid_utils.defaults.clear()
    cid_utils._all_yes = False
    if initial:
        cid_utils.set_parameters(initial)


@pytest.fixture(autouse=True)
def _isolated_parameter_state():
    """Save/restore cid.utils global state around every test."""
    params_backup = dict(cid_utils.params)
    defaults_backup = dict(cid_utils.defaults)
    yes_backup = cid_utils._all_yes
    reset_parameters()
    yield
    cid_utils.params.clear()
    cid_utils.params.update(params_backup)
    cid_utils.defaults.clear()
    cid_utils.defaults.update(defaults_backup)
    cid_utils._all_yes = yes_backup


@pytest.fixture(autouse=True)
def _no_prompts():
    """Fail the test if any interactive prompt is invoked during resolution.

    The agent parameter-store resolution must never display the Athena database
    prompt (with its CREATE NEW option) or the workgroup prompt,
    so ``cid.common.get_parameter`` / ``get_yesno_parameter`` are patched to raise.
    """
    def _fail(*args, **kwargs):
        raise AssertionError(
            'an interactive prompt was displayed during agent parameter-store resolution')
    with patch('cid.common.get_parameter', side_effect=_fail), \
         patch('cid.common.get_yesno_parameter', side_effect=_fail):
        yield


def make_dataset(schemas):
    """Dataset-like object exposing the .schemas list (physical table map databases)."""
    return SimpleNamespace(schemas=list(schemas))


def make_cid(databases=(), datasets=None, workgroup_response=None):
    """Build a Cid harness with mocked athena/qs/parameters_controller helpers.

    :param databases: what athena.list_databases returns (the Athena catalog)
    :param datasets: dataset_id -> Dataset-like object exposed as qs.datasets
    :param workgroup_response: get_work_group response (default: a usable workgroup)
    """
    cid_obj = Cid.__new__(Cid)
    cid_obj.__dict__.clear()
    cid_obj.base = SimpleNamespace(
        account_id=ACCOUNT_ID, region=REGION, partition='aws', domain='aws.amazon.com',
        username='test-user', session=MagicMock(name='session'),
    )

    athena = MagicMock(name='athena')
    # MagicMock auto-creates truthy attributes; the resolver's identity checks need
    # the real not-yet-materialized state (class defaults are None)
    athena._DatabaseName = None
    athena._WorkGroup = None
    athena.defaults = {'CatalogName': CATALOG, 'DatabaseName': 'customer_cur_data',
                       'WorkGroup': DEFAULT_WORKGROUP}
    athena.list_databases.return_value = list(databases)
    athena.client.get_work_group.return_value = workgroup_response or USABLE_WORKGROUP
    cid_obj.__dict__['athena'] = athena

    qs = MagicMock(name='qs')
    qs.datasets = dict(datasets) if datasets is not None else {}
    cid_obj.__dict__['qs'] = qs

    cid_obj.__dict__['parameters_controller'] = MagicMock(name='parameters_controller')
    return cid_obj


def assert_never_creates(cid_obj):
    """The resolution chain is read-only: no create/DDL call on the Athena side."""
    assert not cid_obj.athena.client.create_work_group.called
    assert not cid_obj.athena._ensure_workgroup.called  # the path that can create
    assert not cid_obj.athena.query.called              # no DDL statement


# ===========================================================================
#: session-resolved database used unchanged, no inference
# ===========================================================================
class TestSessionResolvedDatabase:
    """: a session-resolved athena-database is used unchanged with no inference."""

    def test_explicit_parameter_used_unchanged_no_inference(self):
        reset_parameters({'athena-database': 'my_explicit_db', 'athena-workgroup': 'my_wg'})
        cid_obj = make_cid(databases=['cid_cur'])
        assert cid_obj._agent_parameter_store_ready() is True
        # no inference call: neither the Athena catalog nor the dataset map is touched
        assert not cid_obj.athena.list_databases.called
        assert not cid_obj.qs.mock_calls
        # the explicit value is used unchanged
        assert cid_utils.get_parameters().get('athena-database') == 'my_explicit_db'
        assert_never_creates(cid_obj)

    def test_materialized_database_name_skips_inference(self):
        """An already-materialized athena._DatabaseName counts as session-resolved."""
        cid_obj = make_cid(databases=['cid_cur'])
        cid_obj.athena._DatabaseName = 'already_materialized_db'
        cid_obj.athena._WorkGroup = 'already_materialized_wg'
        assert cid_obj._agent_parameter_store_ready() is True
        assert not cid_obj.athena.list_databases.called
        assert not cid_obj.qs.mock_calls
        assert not cid_obj.athena.client.get_work_group.called
        # nothing is written over the materialized session state
        assert 'athena-database' not in cid_utils.get_parameters()
        assert_never_creates(cid_obj)


# ===========================================================================
#a: well-known CID database names
# ===========================================================================
class TestWellKnownNameInference:
    """a: well-known CID names present in the Athena catalog win, in order."""

    def test_cid_cur_hit_sets_athena_database(self):
        cid_obj = make_cid(databases=['some_other_db', 'cid_cur', 'cid_data_export'])
        assert cid_obj._agent_parameter_store_ready() is True
        cid_obj.athena.list_databases.assert_called_once_with(catalog_name=CATALOG)
        assert cid_utils.get_parameters().get('athena-database') == 'cid_cur'
        # the dataset-map fallback is never reached
        assert not cid_obj.qs.mock_calls
        assert_never_creates(cid_obj)

    def test_cid_data_export_fallback_when_cid_cur_absent(self):
        cid_obj = make_cid(databases=['some_other_db', 'cid_data_export'])
        assert cid_obj._agent_parameter_store_ready() is True
        assert cid_utils.get_parameters().get('athena-database') == 'cid_data_export'
        assert not cid_obj.qs.mock_calls
        assert_never_creates(cid_obj)


# ===========================================================================
#b + 20.7: dataset physical-table-map inference
# ===========================================================================
class TestDatasetMapInference:
    """b, 20.7: majority database from dataset schemas, lexicographic tie-break."""

    def test_majority_database_wins_and_selection_is_logged(self, caplog):
        datasets = {
            'd1': make_dataset(['db_beta']),
            'd2': make_dataset(['db_alpha']),
            'd3': make_dataset(['db_alpha']),
        }
        cid_obj = make_cid(databases=['unrelated_db'], datasets=datasets)
        with caplog.at_level(logging.DEBUG, logger='cid.common'):
            assert cid_obj._agent_parameter_store_ready() is True
        # majority wins: db_alpha is referenced by 2 of 3 datasets
        assert cid_utils.get_parameters().get('athena-database') == 'db_alpha'
        # the selection and the competing candidates are logged at debug level (20.7)
        inference_logs = [r.getMessage() for r in caplog.records
                          if INFERENCE_LOG_FRAGMENT in r.getMessage()]
        assert len(inference_logs) == 1
        assert "'db_alpha'" in inference_logs[0]
        assert 'db_beta' in inference_logs[0]  # competing candidate named
        assert_never_creates(cid_obj)

    def test_tie_breaks_by_ascending_lexicographic_order(self):
        datasets = {
            'd1': make_dataset(['db_zeta']),
            'd2': make_dataset(['db_alpha']),
        }
        cid_obj = make_cid(databases=[], datasets=datasets)
        assert cid_obj._agent_parameter_store_ready() is True
        assert cid_utils.get_parameters().get('athena-database') == 'db_alpha'
        assert_never_creates(cid_obj)


# ===========================================================================
#: once-per-invocation caching
# ===========================================================================
class TestOncePerInvocationCaching:
    """: the resolution work runs at most once per command invocation."""

    def test_successful_resolution_runs_at_most_once(self):
        cid_obj = make_cid(databases=['cid_cur'])
        assert cid_obj._agent_parameter_store_ready() is True
        assert cid_obj._agent_parameter_store_ready() is True
        assert cid_obj.athena.list_databases.call_count == 1
        assert cid_obj.athena.client.get_work_group.call_count == 1

    def test_skip_outcome_is_cached_too(self):
        cid_obj = make_cid(databases=[], datasets={})
        assert cid_obj._agent_parameter_store_ready() is False
        assert cid_obj._agent_parameter_store_ready() is False
        assert cid_obj.athena.list_databases.call_count == 1

    def test_load_and_dump_reuse_the_cached_result(self):
        cid_obj = make_cid(databases=['cid_cur'])
        cid_obj.parameters_controller.load_parameters.return_value = {}
        cid_obj._load_agent_default_parameters(AGENT_KEY)
        cid_obj._dump_agent_default_parameters(AGENT_KEY)
        # inference lookups happened exactly once across both store operations
        assert cid_obj.athena.list_databases.call_count == 1
        assert cid_obj.athena.client.get_work_group.call_count == 1


# ===========================================================================
#: skip-on-failure — no prompt, single debug log, load/persist skipped
# ===========================================================================
class TestSkipOnFailure:
    """: inference/workgroup failure silently skips the Parameter_Store."""

    def _assert_silent_skip(self, cid_obj, caplog):
        """Load + dump both return without touching the controller; one debug skip log."""
        with caplog.at_level(logging.DEBUG, logger='cid.common'):
            cid_obj._load_agent_default_parameters(AGENT_KEY)
            cid_obj._dump_agent_default_parameters(AGENT_KEY)
        # the controller is never touched: no load, no persist
        assert not cid_obj.parameters_controller.mock_calls
        # exactly one debug skip log per invocation
        skip_logs = [r for r in caplog.records if SKIP_LOG_FRAGMENT in r.getMessage()]
        assert len(skip_logs) == 1
        assert skip_logs[0].levelno == logging.DEBUG
        # nothing was prompted for (autouse _no_prompts guards) and nothing created
        assert_never_creates(cid_obj)

    def test_no_candidates_anywhere(self, caplog):
        cid_obj = make_cid(databases=[], datasets={})
        self._assert_silent_skip(cid_obj, caplog)

    def test_access_denied_on_list_databases(self, caplog):
        cid_obj = make_cid()
        cid_obj.athena.list_databases.side_effect = Exception(
            'AccessDeniedException: not authorized to perform athena:ListDatabases')
        self._assert_silent_skip(cid_obj, caplog)

    def test_default_workgroup_disabled(self, caplog):
        cid_obj = make_cid(databases=['cid_cur'], workgroup_response={'WorkGroup': {
            'State': 'DISABLED',
            'Configuration': {'ResultConfiguration': {'OutputLocation': 's3://bucket/'}},
        }})
        self._assert_silent_skip(cid_obj, caplog)

    def test_default_workgroup_without_output_location(self, caplog):
        cid_obj = make_cid(databases=['cid_cur'], workgroup_response={'WorkGroup': {
            'State': 'ENABLED',
            'Configuration': {'ResultConfiguration': {}},
        }})
        self._assert_silent_skip(cid_obj, caplog)


# ===========================================================================
#: non-interactive workgroup resolution
# ===========================================================================
class TestWorkgroupResolution:
    """: explicit athena-workgroup param, else the read-only default check."""

    def test_explicit_workgroup_parameter_honored(self):
        reset_parameters({'athena-workgroup': 'my-workgroup'})
        cid_obj = make_cid(databases=['cid_cur'])
        assert cid_obj._agent_parameter_store_ready() is True
        # the explicit parameter short-circuits the default-workgroup check
        assert not cid_obj.athena.client.get_work_group.called
        assert cid_utils.get_parameters().get('athena-workgroup') == 'my-workgroup'
        assert_never_creates(cid_obj)

    def test_default_workgroup_usability_checked_read_only_and_set(self):
        cid_obj = make_cid(databases=['cid_cur'])
        assert cid_obj._agent_parameter_store_ready() is True
        # a single read-only usability check on the helper's default workgroup
        cid_obj.athena.client.get_work_group.assert_called_once_with(
            WorkGroup=DEFAULT_WORKGROUP)
        assert cid_utils.get_parameters().get('athena-workgroup') == DEFAULT_WORKGROUP
        # never any workgroup creation
        assert_never_creates(cid_obj)


# ===========================================================================
#: the deploy path (Athena helper) is untouched
# ===========================================================================
class TestDeployPathUntouched:
    """: the guard lives only on the agent helpers; the Athena helper is unmodified."""

    def test_athena_helper_has_no_agent_guard_reference(self):
        source = inspect.getsource(athena_module)
        # the agent-only guard never leaked into the Athena helper
        assert '_agent_parameter_store_ready' not in source
        assert '_resolve_agent_parameter_store' not in source
        assert '_infer_agent_athena_database' not in source
        # deploy's interactive resolution — including the CREATE NEW option — is intact
        assert '(CREATE NEW)' in source
        assert '(create new)' in source

    def test_guard_exists_only_on_the_agent_store_helpers(self):
        # the agent load/persist helpers are the only guarded call sites
        for helper in (Cid._load_agent_default_parameters, Cid._dump_agent_default_parameters):
            assert '_agent_parameter_store_ready' in inspect.getsource(helper)
        # the dashboard-path equivalents are unguarded (existing behavior preserved)
        for helper in (Cid.load_default_parameters, Cid.dump_default_parameters):
            assert '_agent_parameter_store_ready' not in inspect.getsource(helper)


# ===========================================================================
#: agent and dashboard rows coexist in the same store
# ===========================================================================
class TestStoreCoexistence:
    """: the same cid_parameters store, context-keyed by agent id, dump-only."""

    def test_dump_uses_same_controller_with_agent_context_and_touches_nothing_else(self):
        reset_parameters({'some-agent-param': 'value', 'profile-name': 'secret-profile'})
        cid_obj = make_cid(databases=['cid_cur'])
        cid_obj._dump_agent_default_parameters('my-agent')
        # exactly one call reached the shared controller: the context-keyed dump —
        # no dashboard-context row is read, altered, or removed by the handler
        assert len(cid_obj.parameters_controller.mock_calls) == 1
        name, args, kwargs = cid_obj.parameters_controller.mock_calls[0]
        assert name == 'dump_parameters'
        assert kwargs.get('context') == 'my-agent'
        dumped = args[0]
        assert dumped.get('some-agent-param') == 'value'
        # the stop_list is respected: credentials and athena-* are never stored
        assert 'profile-name' not in dumped
        assert 'athena-database' not in dumped
        assert 'athena-workgroup' not in dumped
