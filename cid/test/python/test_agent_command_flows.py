""" Tests for the create-agent / delete-agent / list-agents command flows.

Each command is exercised through its UNDECORATED handler (``Cid.<cmd>.__wrapped__``)
with mocked qs/space/agent helpers.
"""

import io
import re
import contextlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st
import cid.utils as cid_utils
from cid.common import Cid
from cid.exceptions import CidError, CidCritical
from cid.helpers.quicksight.agent_logic import (
    CID_MANAGED_MARKER,
    CID_PROVENANCE_TAG_KEY,
    CID_PROVENANCE_TAG_VALUE,
)
from cid.exceptions import CidError
from unittest.mock import MagicMock
from cid.helpers.quicksight.agent_logic import DEPLOYED_MARK, build_agent_listing
from InquirerPy.separator import Separator


# ======================================================================
# from test_create_agent_flow.py
# ======================================================================


ACCOUNT_ID = '123456789012'


REGION = 'us-east-1'


PARTITION = 'aws'


DOMAIN = 'aws.amazon.com'


USERNAME = 'test-user'


USER_ARN = f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:user/default/{USERNAME}'


AGENT_ID = 'test-agent'


SPACE_KEY = 'test-space'


SPACE_ARN = f'arn:{PARTITION}:quicksight:{REGION}:{ACCOUNT_ID}:space/{SPACE_KEY}'


AGENT_ARN = f'arn:{PARTITION}:quicksight:{REGION}:{ACCOUNT_ID}:agent/{AGENT_ID}'


create_agent = Cid.create_agent.__wrapped__


QS_READ_ONLY_ALLOWED = {'ensure_subscription', 'describe_user', 'describe_group', 'list_dashboards'}


AGENT_MUTATING_METHODS = ('create_or_update', 'delete', 'grant_owner', 'write_provenance', 'wait_active', 'repair_space_associations')


def dash_id(key):
    return f'{key}-id'


def dash_arn(key):
    return f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:dashboard/{key}-id'


def dataset_arn(key):
    return f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:dataset/{key}-id'


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


def make_exceptions():
    """Real exception subclasses for the mocked quicksight client."""
    ns = SimpleNamespace()
    for name in ('ResourceNotFoundException', 'ResourceExistsException',
                 'AccessDeniedException', 'ConflictException', 'ClientError'):
        setattr(ns, name, type(name, (Exception,), {}))
    return ns


def make_definition(required=(), optional=(), datasets=(), knowledge_bases=(),
                    lifecycle='PUBLISHED', name='Test Agent', agent_id=AGENT_ID,
                    space_keys=(SPACE_KEY,)):
    """Build a cap-valid catalog agent entry."""
    depends = {}
    if space_keys:
        depends['spaces'] = list(space_keys)
    if required:
        depends['dashboards'] = list(required)
    if optional:
        depends['optionalDashboards'] = list(optional)
    if datasets:
        depends['datasets'] = list(datasets)
    if knowledge_bases:
        depends['knowledgeBases'] = list(knowledge_bases)
    return {
        'name': name,
        'agentId': agent_id,
        'category': 'FinOps',
        'persona': {
            'identity': 'Cost optimization advisor',
            'customInstructions': 'Answer using the attached dashboards.',
            'tone': 'Professional',
            'outputStyle': 'Concise text',
            'responseLength': 'Short answers',
        },
        'starterPrompts': ['What is my spend?'],
        'welcomeMessage': 'Welcome to the advisor.',
        'lifecycle': lifecycle,
        'dependsOn': depends,
    }


def make_create_cid(definition, present=(), present_datasets=(), user_role='AUTHOR_PRO',
             existing_agent=None, resource_tags=()):
    """Build a Cid harness with mocked qs/space/agent/parameters_controller.

    :param definition: the catalog agent entry (registered under its agentId)
    :param present: dependency dashboard catalog keys deployed in the account
    :param present_datasets: dependency dataset catalog keys deployed in the account
    :param user_role: role returned by qs.describe_user
    :param existing_agent: DescribeAgent 'Agent' dict returned by agent.get, or None
    :param resource_tags: Tags list returned by list_tags_for_resource (provenance)
    """
    cid_obj = Cid.__new__(Cid)
    cid_obj.__dict__.clear()
    excs = make_exceptions()

    depends = definition.get('dependsOn') or {}
    resources = {'agents': {definition['agentId']: definition},
                 'spaces': {SPACE_KEY: {'name': 'Test Space', 'description': 'CID test space'}},
                 'dashboards': {}, 'datasets': {}}
    for key in list(depends.get('dashboards') or []) + list(depends.get('optionalDashboards') or []):
        resources['dashboards'][key] = {'dashboardId': dash_id(key)}
    for key in depends.get('datasets') or []:
        resources['datasets'][key] = {'data': {'DataSetId': f'{key}-id'}}
    cid_obj.resources = resources

    cid_obj.base = SimpleNamespace(
        account_id=ACCOUNT_ID, region=REGION, partition=PARTITION, domain=DOMAIN,
        username=USERNAME, session=MagicMock(name='session'),
    )

    qs = MagicMock(name='qs')
    qs.ensure_subscription.return_value = None
    qs.describe_user.return_value = {'Arn': USER_ARN, 'Role': user_role, 'UserName': USERNAME}
    qs.list_dashboards.return_value = [
        {'DashboardId': dash_id(key), 'Arn': dash_arn(key)} for key in present
    ]
    qs.datasets = {f'{key}-id': SimpleNamespace(arn=dataset_arn(key)) for key in present_datasets}
    cid_obj.__dict__['qs'] = qs

    space = MagicMock(name='space')
    space.client.exceptions = excs
    space.client.meta.method_to_api_mapping = {'create_space': 'CreateSpace', 'create_agent': 'CreateAgent'}
    space.client.list_tags_for_resource.return_value = {'Tags': list(resource_tags)}
    space.get.return_value = None
    space.create_or_update.return_value = SPACE_ARN
    space.find_by_name.return_value = []
    space.update_resources.return_value = []
    cid_obj.__dict__['space'] = space

    agent = MagicMock(name='agent')
    agent.client.exceptions = excs
    agent.client.list_agents.return_value = {'AgentSummaries': []}  # enumeration-only, no NextToken
    agent.get.return_value = existing_agent
    agent.create_or_update.return_value = {
        'agentId': definition['agentId'], 'arn': AGENT_ARN,
        'action': 'created' if existing_agent is None else 'updated',
    }
    cid_obj.__dict__['agent'] = agent

    parameters_controller = MagicMock(name='parameters_controller')
    parameters_controller.load_parameters.return_value = {}
    cid_obj.__dict__['parameters_controller'] = parameters_controller

    # deploy-engine / data-layer sentinels: any call on these is out of scope
    for sentinel in ('athena', 'glue', 'organizations', 's3', 'cur1', 'cur2', 'iam', 'cfn'):
        cid_obj.__dict__[sentinel] = MagicMock(name=sentinel)
    return cid_obj


def run_create(cid_obj, **kwargs):
    """Run the undecorated handler capturing stdout; returns (result, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = create_agent(cid_obj, **kwargs)
    return result, buffer.getvalue()


def run_create_raising(cid_obj, exception_type, **kwargs):
    """Run the handler expecting an exception; returns (excinfo, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        with pytest.raises(exception_type) as excinfo:
            create_agent(cid_obj, **kwargs)
    return excinfo, buffer.getvalue()


update_agent = Cid.update_agent.__wrapped__


def run_update(cid_obj, **kwargs):
    """Run the undecorated update-agent handler capturing stdout; returns (result, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = update_agent(cid_obj, **kwargs)
    return result, buffer.getvalue()


def run_update_raising(cid_obj, exception_type, **kwargs):
    """Run the update-agent handler expecting an exception; returns (excinfo, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        with pytest.raises(exception_type) as excinfo:
            update_agent(cid_obj, **kwargs)
    return excinfo, buffer.getvalue()


def dashboard_update_calls(cid_obj):
    """The space.update_resources calls with resource_type='DASHBOARD'."""
    return [call for call in cid_obj.space.update_resources.call_args_list
            if call.kwargs.get('resource_type') == 'DASHBOARD']


def assert_nothing_created(cid_obj):
    """No Space or Agent create/update/grant/provenance call was made."""
    assert not cid_obj.space.create_or_update.called
    assert not cid_obj.space.update_resources.called
    assert not cid_obj.space.grant_owner.called
    assert not cid_obj.space.write_provenance.called
    for method in AGENT_MUTATING_METHODS:
        assert not getattr(cid_obj.agent, method).called


def assert_no_deploy_or_data_layer_calls(cid_obj):
    """Property 13 core assertion: zero deploy-engine and zero data-layer calls."""
    qs_methods = {call[0].split('.')[0] for call in cid_obj.qs.method_calls}
    assert qs_methods <= QS_READ_ONLY_ALLOWED, (
        f'create-agent must only issue read-only qs calls, got: {qs_methods - QS_READ_ONLY_ALLOWED}')
    for sentinel in ('athena', 'glue', 'organizations', 's3', 'cur1', 'cur2', 'iam', 'cfn'):
        helper = cid_obj.__dict__[sentinel]
        assert not helper.mock_calls, f'data-layer helper {sentinel!r} must never be touched: {helper.mock_calls}'


class TestErrorPropagation:
    """: CidError/CidCritical propagate cleanly out of the handler."""

    def test_cid_error_from_helper_propagates_unchanged(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        original = CidError('helper-level error')
        cid_obj.agent.create_or_update.side_effect = original
        excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)
        assert excinfo.value is original
        # CidError is re-raised directly, not routed through cleanup
        assert not cid_obj.agent.delete.called

    def test_cid_critical_from_helper_propagates_unchanged(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        original = CidCritical('critical helper error')
        cid_obj.agent.create_or_update.side_effect = original
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        assert excinfo.value is original
        assert not cid_obj.agent.delete.called


class TestSubscriptionDetectAndRequire:
    """: subscription failure is CidCritical and never activates."""

    def test_subscription_failure_raises_cid_critical_and_creates_nothing(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.qs.ensure_subscription.side_effect = CidCritical('subscription not active')
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert 'Enterprise' in message
        assert 'will not activate' in message
        assert_nothing_created(cid_obj)
        # read-only detection: ensure_subscription is the ONLY qs call, no activation
        assert [call[0] for call in cid_obj.qs.method_calls] == ['ensure_subscription']


class TestCleanupOnFailure:
    """: best-effort delete of the partially created agent, then re-raise."""

    def test_generic_create_failure_triggers_cleanup_delete_and_reraises(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.agent.create_or_update.side_effect = RuntimeError('boom')
        excinfo, _ = run_create_raising(cid_obj, RuntimeError, agent_id=AGENT_ID)
        assert 'boom' in str(excinfo.value)
        # cleanup delete goes through Agent.delete, which retries ConflictException
        # internally (, covered by the Agent helper tests)
        cid_obj.agent.delete.assert_called_once_with(AGENT_ID)

    def test_cleanup_delete_failure_is_tolerated_and_original_error_reraised(self):
        """ plumbing: even when the ConflictException retry loop ultimately
        fails, the cleanup stays best-effort and the original error surfaces."""
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.agent.create_or_update.side_effect = RuntimeError('boom')
        cid_obj.agent.delete.side_effect = CidError('agent still busy (ConflictException) after 300 seconds')
        excinfo, _ = run_create_raising(cid_obj, RuntimeError, agent_id=AGENT_ID)
        assert 'boom' in str(excinfo.value)
        cid_obj.agent.delete.assert_called_once_with(AGENT_ID)

    def test_no_cleanup_delete_when_agent_pre_existed(self):
        """A pre-existing CID-managed agent is never deleted by cleanup."""
        existing = {'Arn': AGENT_ARN, 'Description': f'mine {CID_MANAGED_MARKER}'}
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',),
                           existing_agent=existing)
        cid_obj.agent.create_or_update.side_effect = RuntimeError('boom')
        reset_parameters({'update': 'yes'})
        run_create_raising(cid_obj, RuntimeError, agent_id=AGENT_ID)
        assert not cid_obj.agent.delete.called


class TestResourceExistsSuccessPlumbing:
    """: ResourceExistsException during create is success.

    The tolerate-ResourceExists behavior lives in Agent.create_or_update (covered by
    test_agent_helper.py); here we assert the handler treats the helper's 'created'
    result after a tolerated ResourceExistsException as full success.
    """

    def test_created_result_after_tolerated_resource_exists_completes_the_flow(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        # exactly what Agent.create_or_update returns after a tolerated ResourceExists
        cid_obj.agent.create_or_update.return_value = {
            'agentId': AGENT_ID, 'arn': AGENT_ARN, 'action': 'created'}
        result, output = run_create(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        assert 'Congratulations' in output
        cid_obj.agent.write_provenance.assert_called_once_with(AGENT_ARN, AGENT_ID)
        assert not cid_obj.agent.delete.called


class TestPreflightDependencySummary:
    """The pre-flight prints the same ✓/✗ dependency summary list-agents shows,
    so create/update surface the full dependency picture, not just warnings."""

    def test_summary_line_shows_present_and_missing_dependencies(self):
        cid_obj = make_create_cid(
            make_definition(required=('dash-a', 'dash-b'), optional=('dash-c',)),
            present=('dash-a',))
        _, output = run_create(cid_obj, agent_id=AGENT_ID)
        plain = re.sub(r'\033\[[0-9;]*m', '', output)
        assert 'Dependencies of agent' in plain
        assert '✓ dash-a' in plain
        assert '✗ dash-b' in plain
        assert 'optional:' in plain and '✗ dash-c' in plain


class TestProgressOutput:
    """The apply flow prints a headline plus indented outcome facts (Space /
    Knowledge / Agent / Status), matching the tool's section-and-facts style."""

    def test_create_prints_headline_and_outcome_facts(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        _, output = run_create(cid_obj, agent_id=AGENT_ID)
        plain = re.sub(r'\033\[[0-9;]*m', '', output)
        assert 'Creating agent Test Agent...' in plain
        assert '\tKnowledge: 1 dashboard linked' in plain
        assert f'\tAgent: created ({AGENT_ID})' in plain
        assert '\tStatus: waiting to become ACTIVE' in plain
        assert '\tStatus: ACTIVE (' in plain

    def test_no_wait_lines_for_preview_lifecycle(self):
        cid_obj = make_create_cid(
            make_definition(required=('dash-a',), lifecycle='PREVIEW'), present=('dash-a',))
        _, output = run_create(cid_obj, agent_id=AGENT_ID)
        assert 'waiting to become ACTIVE' not in output


class TestBringYourOwnSpace:
    """: --space select/create and SearchSpaces resolution."""

    def test_single_name_match_reuses_the_existing_space(self):
        """: a single SearchSpaces match is reused, not re-created."""
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        existing_arn = f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:space/existing-space'
        cid_obj.space.find_by_name.return_value = ['existing-space']
        cid_obj.space.get.return_value = {
            'spaceArn': existing_arn, 'name': 'My Space',
            'description': f'managed {CID_MANAGED_MARKER}',
        }
        result, _ = run_create(cid_obj, agent_id=AGENT_ID, space_name='My Space')
        assert result == AGENT_ID
        cid_obj.space.find_by_name.assert_called_once_with('My Space')
        # CID-managed reuse path updates the existing space id, never a derived one
        assert cid_obj.space.create_or_update.call_args.args[0] == 'existing-space'
        assert dashboard_update_calls(cid_obj)[0].args[0] == 'existing-space'

    def test_no_name_match_creates_space_with_derived_id(self):
        """: non-existent --space name creates a Space with derive_space_id."""
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.space.find_by_name.return_value = []
        result, _ = run_create(cid_obj, agent_id=AGENT_ID, space_name='My Space')
        assert result == AGENT_ID
        args = cid_obj.space.create_or_update.call_args
        assert args.args[0] == 'My-Space'   # deterministic sanitized id
        assert args.args[1] == 'My Space'   # display name preserved
        cid_obj.space.write_provenance.assert_called_once()
        cid_obj.space.grant_owner.assert_called_once_with('My-Space', USER_ARN)

    def test_ambiguous_name_match_raises_cid_error_listing_ids(self):
        """: multiple exact matches raise a CidError listing the matching ids."""
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.space.find_by_name.return_value = ['space-one', 'space-two']
        excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID, space_name='My Space')
        message = str(excinfo.value)
        assert 'space-one' in message and 'space-two' in message
        assert not cid_obj.space.create_or_update.called
        assert not cid_obj.agent.create_or_update.called


class TestKnowledgeBaseAttachment:
    """: valid KB ARNs attach; unresolvable references warn and skip."""

    def test_valid_arns_attach_and_non_arn_reference_warns(self):
        kb_arn = f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:knowledge-base/kb-one'
        definition = make_definition(required=('dash-a',),
                                     knowledge_bases=(kb_arn, 'my-kb-note'))
        cid_obj = make_create_cid(definition, present=('dash-a',))
        result, output = run_create(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        kb_calls = [call for call in cid_obj.space.update_resources.call_args_list
                    if call.kwargs.get('resource_type') == 'KNOWLEDGE_BASE']
        assert len(kb_calls) == 1
        assert kb_calls[0].args[1] == [kb_arn]
        assert 'my-kb-note' in output and 'Warning' in output

    def test_no_kb_update_call_when_nothing_resolvable(self):
        definition = make_definition(required=('dash-a',), knowledge_bases=('not-an-arn',))
        cid_obj = make_create_cid(definition, present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        kb_calls = [call for call in cid_obj.space.update_resources.call_args_list
                    if call.kwargs.get('resource_type') == 'KNOWLEDGE_BASE']
        assert kb_calls == []


class TestPrincipalResolution:
    """: Author Pro entitlement and registered-principal checks."""

    def test_missing_author_pro_role_raises_cid_critical(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',),
                           user_role='AUTHOR')
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        assert 'Author Pro' in str(excinfo.value)
        assert_nothing_created(cid_obj)

    def test_unregistered_principal_raises_before_any_grant(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.qs.describe_user.return_value = None
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert USERNAME in message
        assert 'register' in message.lower()
        assert not cid_obj.space.grant_owner.called
        assert not cid_obj.agent.grant_owner.called
        assert_nothing_created(cid_obj)


class TestGenAiAvailabilityAndAccessDenied:
    """: gen-AI availability pre-check and AccessDenied handling."""

    def test_missing_genai_operations_raise_cid_critical_naming_region_and_partition(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        cid_obj.space.client.meta.method_to_api_mapping = {'describe_dashboard': 'DescribeDashboard'}
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert REGION in message and PARTITION in message
        assert_nothing_created(cid_obj)

    def test_access_denied_raises_cid_critical_naming_action_and_permission(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        denied = cid_obj.space.client.exceptions.AccessDeniedException('denied')
        denied.operation_name = 'UpdateSpaceResources'
        cid_obj.space.update_resources.side_effect = denied
        excinfo, _ = run_create_raising(cid_obj, CidCritical, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert 'UpdateSpaceResources' in message
        assert 'quicksight:UpdateSpaceResources' in message
        # no further create or modify calls after the denial
        assert not cid_obj.agent.create_or_update.called
        assert not cid_obj.agent.grant_owner.called


class TestParameterPersistence:
    """: per-agent parameter persistence."""

    def test_parameters_persisted_keyed_by_agent_id(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        dump = cid_obj.parameters_controller.dump_parameters
        dump.assert_called_once()
        assert dump.call_args.kwargs.get('context') == AGENT_ID


class TestConflictGuard:
    """: non-CID collisions are refused; CID-managed agents proceed."""

    def test_non_cid_existing_agent_is_refused_without_mutation(self):
        existing = {'Arn': AGENT_ARN, 'Description': 'hand-made agent'}
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',),
                           existing_agent=existing)
        excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)
        assert 'Refusing' in str(excinfo.value)
        for method in AGENT_MUTATING_METHODS:
            assert not getattr(cid_obj.agent, method).called

    def test_cid_managed_existing_agent_proceeds_with_update_path(self):
        existing = {'Arn': AGENT_ARN, 'Description': f'agent {CID_MANAGED_MARKER}'}
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',),
                           existing_agent=existing)
        reset_parameters({'update': 'yes'})
        result, output = run_create(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        cid_obj.agent.create_or_update.assert_called_once()
        assert 'updated' in output


class TestNonCidSpaceReuse:
    """: a non-CID --space is reused additively, never mutated itself."""

    def test_non_cid_space_is_not_renamed_tagged_or_repermissioned(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        shared_arn = f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:space/shared-space'
        cid_obj.space.find_by_name.return_value = ['shared-space']
        cid_obj.space.get.return_value = {
            'spaceArn': shared_arn, 'name': 'Shared Space', 'description': 'user space'}
        result, _ = run_create(cid_obj, agent_id=AGENT_ID, space_name='Shared Space')
        assert result == AGENT_ID
        assert not cid_obj.space.create_or_update.called
        assert not cid_obj.space.write_provenance.called
        assert not cid_obj.space.grant_owner.called
        # resources are still added additively to the reused space
        calls = dashboard_update_calls(cid_obj)
        assert len(calls) == 1 and calls[0].args[0] == 'shared-space'
        assert calls[0].args[1] == [dash_arn('dash-a')]


class TestDatasetKnowledge:
    """: present datasets attach as DATA_SET; missing ones warn; zero creates."""

    def test_present_dataset_attaches_missing_dataset_warns_no_creates(self):
        definition = make_definition(required=('dash-a',),
                                     datasets=('ds-present', 'ds-missing'))
        cid_obj = make_create_cid(definition, present=('dash-a',), present_datasets=('ds-present',))
        result, output = run_create(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        dataset_calls = [call for call in cid_obj.space.update_resources.call_args_list
                         if call.kwargs.get('resource_type') == 'DATA_SET']
        assert len(dataset_calls) == 1
        assert dataset_calls[0].args[1] == [dataset_arn('ds-present')]
        assert 'ds-missing' in output and 'Warning' in output
        # never create a dataset: only read-only qs calls happened
        qs_methods = {call[0].split('.')[0] for call in cid_obj.qs.method_calls}
        assert qs_methods <= QS_READ_ONLY_ALLOWED

    def test_no_dataset_update_call_when_none_present(self):
        definition = make_definition(required=('dash-a',), datasets=('ds-missing',))
        cid_obj = make_create_cid(definition, present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        dataset_calls = [call for call in cid_obj.space.update_resources.call_args_list
                         if call.kwargs.get('resource_type') == 'DATA_SET']
        assert dataset_calls == []


class TestDualProvenanceAndLifecycle:
    """: dual provenance writes and PUBLISHED wait_active."""

    def test_dual_provenance_written_on_created_space_and_agent(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        cid_obj.space.write_provenance.assert_called_once_with(SPACE_ARN, SPACE_KEY)
        cid_obj.agent.write_provenance.assert_called_once_with(AGENT_ARN, AGENT_ID)

    def test_wait_active_called_for_published_lifecycle(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',), lifecycle='PUBLISHED'),
                           present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        cid_obj.agent.wait_active.assert_called_once_with(AGENT_ID)

    def test_wait_active_not_called_for_preview_lifecycle(self):
        cid_obj = make_create_cid(make_definition(required=('dash-a',), lifecycle='PREVIEW'),
                           present=('dash-a',))
        run_create(cid_obj, agent_id=AGENT_ID)
        assert not cid_obj.agent.wait_active.called

    def test_wait_active_failed_fail_fast_surfaces(self):
        """: FAILED status raised by wait_active surfaces from the handler."""
        cid_obj = make_create_cid(make_definition(required=('dash-a',), lifecycle='PUBLISHED'),
                           present=('dash-a',))
        cid_obj.agent.wait_active.side_effect = CidError(
            f"Agent {AGENT_ID!r} entered FAILED status while waiting for ACTIVE. "
            "ErrorMessage: model error")
        excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert 'FAILED' in message and 'model error' in message


PROPERTY_SETTINGS = settings(
    max_examples=100, deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


required_flags = st.lists(st.booleans(), min_size=1, max_size=4)


optional_flags = st.lists(st.booleans(), min_size=0, max_size=3)


def keys_from_flags(req_flags, opt_flags):
    """Derive (required, optional, present) catalog key lists from presence flags."""
    required = [f'req-{i}' for i in range(len(req_flags))]
    optional = [f'opt-{i}' for i in range(len(opt_flags))]
    present = ([key for key, flag in zip(required, req_flags) if flag]
               + [key for key, flag in zip(optional, opt_flags) if flag])
    return required, optional, present


@PROPERTY_SETTINGS
@given(req_flags=required_flags, opt_flags=optional_flags)
def test_property_13_zero_deploy_and_zero_data_layer_calls(req_flags, opt_flags):
    """Across zero/partial/full dependency states, create-agent only reads via
    ListDashboards and creates or updates the Space and Agent — it never invokes any
    dashboard-deploy engine or data-layer (CUR/Data Exports/Data Collection) call."""
    reset_parameters()
    required, optional, present = keys_from_flags(req_flags, opt_flags)
    definition = make_definition(required=required, optional=optional)
    cid_obj = make_create_cid(definition, present=present)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        if present:
            create_agent(cid_obj, agent_id=AGENT_ID)
        else:
            with pytest.raises(CidError):  # zero-present guidance error path
                create_agent(cid_obj, agent_id=AGENT_ID)
    assert_no_deploy_or_data_layer_calls(cid_obj)


@PROPERTY_SETTINGS
@given(
    n_required=st.integers(min_value=1, max_value=4),
    n_optional=st.integers(min_value=0, max_value=3),
    n_unrelated=st.integers(min_value=0, max_value=3),
)
def test_property_10_zero_present_raises_guidance_error_and_creates_nothing(
        n_required, n_optional, n_unrelated):
    """With every dependency dashboard absent (even when unrelated dashboards are
    deployed), the handler stops with a CidError carrying the CID/CUDOS and Data
    Collection guidance, names no cid-cmd command, and creates nothing."""
    reset_parameters()
    required = [f'req-{i}' for i in range(n_required)]
    optional = [f'opt-{i}' for i in range(n_optional)]
    definition = make_definition(required=required, optional=optional)
    cid_obj = make_create_cid(definition, present=())
    # unrelated deployed dashboards never satisfy catalog dependencies
    cid_obj.qs.list_dashboards.return_value = [
        {'DashboardId': f'unrelated-{i}',
         'Arn': f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:dashboard/unrelated-{i}'}
        for i in range(n_unrelated)
    ]
    excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)
    message = str(excinfo.value)
    assert 'https://catalog.workshops.aws/awscid/en-US' in message   # CID/CUDOS guidance
    assert 'data-collection' in message                              # Data Collection guidance
    assert 'cid-cmd' not in message
    assert_nothing_created(cid_obj)


@PROPERTY_SETTINGS
@given(req_flags=required_flags, opt_flags=optional_flags)
def test_property_11_proceeds_attaching_exactly_the_present_set(req_flags, opt_flags):
    """With >=1 dependency dashboard present, the Space update receives exactly
    the present dashboard ARNs (never an absent one) and the Agent is built."""
    if not any(req_flags) and not any(opt_flags):
        req_flags = [True] + list(req_flags[1:])  # force at least one present
    reset_parameters()
    required, optional, present = keys_from_flags(req_flags, opt_flags)
    definition = make_definition(required=required, optional=optional)
    cid_obj = make_create_cid(definition, present=present)
    result, _ = run_create(cid_obj, agent_id=AGENT_ID)
    assert result == AGENT_ID
    calls = dashboard_update_calls(cid_obj)
    assert len(calls) == 1
    attached = calls[0].args[1]
    assert set(attached) == {dash_arn(key) for key in present}
    assert len(attached) == len(set(attached))  # de-duplicated
    cid_obj.agent.create_or_update.assert_called_once()


@PROPERTY_SETTINGS
@given(req_flags=required_flags, opt_flags=optional_flags)
def test_property_12_missing_dependency_warnings_carry_correct_guidance(req_flags, opt_flags):
    """Each missing Required dashboard warns with the CID/CUDOS guidance, each
    missing Optional dashboard warns with the Data Collection guidance, no warning
    names a cid-cmd command, and nothing is deployed."""
    if not any(req_flags) and not any(opt_flags):
        req_flags = [True] + list(req_flags[1:])  # a proceeding run_create needs >=1 present
    reset_parameters()
    required, optional, present = keys_from_flags(req_flags, opt_flags)
    missing_required = [key for key in required if key not in present]
    missing_optional = [key for key in optional if key not in present]
    definition = make_definition(required=required, optional=optional)
    cid_obj = make_create_cid(definition, present=present)
    result, output = run_create(cid_obj, agent_id=AGENT_ID)
    assert result == AGENT_ID
    lines = output.splitlines()
    for key in missing_required:
        matching = [line for line in lines if key in line and 'required dashboard' in line]
        assert len(matching) == 1, f'expected one required-dashboard warning for {key!r}'
        assert 'https://catalog.workshops.aws/awscid/en-US)' in matching[0]
        assert 'cid-cmd' not in matching[0]
    for key in missing_optional:
        matching = [line for line in lines if key in line and 'optional dashboard' in line]
        assert len(matching) == 1, f'expected one optional-dashboard warning for {key!r}'
        assert 'Data Collection' in matching[0]
        assert 'cid-cmd' not in matching[0]
    assert_no_deploy_or_data_layer_calls(cid_obj)


@PROPERTY_SETTINGS
@given(
    has_tag=st.booleans(),
    has_marker=st.booleans(),
    description=st.text(alphabet='abcdefghij 0123456789', max_size=40),
)
def test_property_20_non_cid_agent_collisions_refused_not_mutated(has_tag, has_marker, description):
    """An existing agent with the target id proceeds to the idempotent update path
    if and only if it carries CID-managed provenance (tag OR description marker);
    otherwise it is refused with a CidError and left entirely unmodified."""
    reset_parameters()
    full_description = f'{description} {CID_MANAGED_MARKER}' if has_marker else description
    tags = [{'Key': CID_PROVENANCE_TAG_KEY, 'Value': CID_PROVENANCE_TAG_VALUE}] if has_tag else []
    existing = {'Arn': AGENT_ARN, 'Description': full_description}
    cid_obj = make_create_cid(make_definition(required=('dash-a',)), present=('dash-a',),
                       existing_agent=existing, resource_tags=tags)
    if has_tag or has_marker:  # CID-managed: idempotent update/no-op path proceeds
        reset_parameters({'update': 'yes'})
        result, _ = run_create(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        cid_obj.agent.create_or_update.assert_called_once()
    else:                      # non-CID: refused, zero mutating calls on the agent
        excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)
        assert 'Refusing' in str(excinfo.value)
        for method in AGENT_MUTATING_METHODS:
            assert not getattr(cid_obj.agent, method).called


@PROPERTY_SETTINGS
@given(
    space_id=st.text(alphabet='abcdefghij-0123456789', min_size=1, max_size=20),
    description=st.text(alphabet='abcdefghij 0123456789', max_size=40),
    req_flags=required_flags,
)
def test_property_21_non_cid_space_reuse_never_mutates_the_space(space_id, description, req_flags):
    """Reusing a pre-existing non-CID space adds this agent's resources additively
    while the space itself is never renamed, re-described, tagged (write_provenance),
    or re-permissioned (grant_owner)."""
    if not any(req_flags):
        req_flags = [True] + list(req_flags[1:])
    reset_parameters()
    required, _, present = keys_from_flags(req_flags, [])
    definition = make_definition(required=required)
    cid_obj = make_create_cid(definition, present=present)
    cid_obj.space.find_by_name.return_value = [space_id]
    cid_obj.space.get.return_value = {
        'spaceArn': f'arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:space/{space_id}',
        'name': 'Shared Space',
        'description': description,  # never carries the CID-managed marker
    }
    result, _ = run_create(cid_obj, agent_id=AGENT_ID, space_name='Shared Space')
    assert result == AGENT_ID
    # the space itself is never mutated
    assert not cid_obj.space.create_or_update.called
    assert not cid_obj.space.write_provenance.called
    assert not cid_obj.space.grant_owner.called
    # while this agent's resources are still added additively
    calls = dashboard_update_calls(cid_obj)
    assert len(calls) == 1
    assert calls[0].args[0] == space_id
    assert set(calls[0].args[1]) == {dash_arn(key) for key in present}


@PROPERTY_SETTINGS
@given(
    name=st.text(alphabet='abcdefghijklmnopqrstuvwxyz ABCDEFG', min_size=1, max_size=40)
        .filter(lambda text: text.strip()),
    lifecycle=st.sampled_from(['PUBLISHED', 'PREVIEW']),
)
def test_property_24_creation_completes_and_reports_success(name, lifecycle):
    """For any successful creation, the flow completes, returns the agent id, and
    reports success."""
    reset_parameters()
    definition = make_definition(required=('dash-a',), name=name, lifecycle=lifecycle)
    cid_obj = make_create_cid(definition, present=('dash-a',))
    result, output = run_create(cid_obj, agent_id=AGENT_ID)
    assert result == AGENT_ID
    cid_obj.agent.create_or_update.assert_called_once()
    assert 'Congratulations' in output


@PROPERTY_SETTINGS
@given(
    agent_keys=st.lists(
        st.text(alphabet='abcdefghij', min_size=1, max_size=10),
        min_size=1, max_size=3, unique=True,
    ),
)
def test_property_26_missing_agent_id_reported_by_name_non_interactively(agent_keys):
    """With no --agent-id, no stored default, and no fallback in a non-interactive
    environment, resolution raises an error naming the missing 'agent-id' input."""
    reset_parameters()
    definition = make_definition(required=('dash-a',))
    cid_obj = make_create_cid(definition, present=('dash-a',))
    cid_obj.resources['agents'] = {
        key: {'name': key.title(), 'agentId': key, 'category': 'FinOps'}
        for key in agent_keys
    }
    buffer = io.StringIO()
    with patch('cid.utils.isatty', return_value=False):
        with contextlib.redirect_stdout(buffer):
            with pytest.raises((CidError, CidCritical)) as excinfo:
                create_agent(cid_obj)  # no agent_id supplied
    assert 'agent-id' in str(excinfo.value)
    assert_nothing_created(cid_obj)


# ======================================================================
# from test_delete_agent_flow.py
# ======================================================================


delete_agent = Cid.delete_agent.__wrapped__


ALLOWED_DELETE_CALLS = {'agent.delete', 'space.client.delete_space'}


FORBIDDEN_NAME_FRAGMENTS = (
    'create_dashboard', 'delete_dashboard',
    'create_data_set', 'delete_data_set', 'create_dataset', 'delete_dataset',
    'create_knowledge_base', 'delete_knowledge_base',
    'create_research', 'delete_research',
)


def agent_arn(agent_id):
    return f'arn:{PARTITION}:quicksight:{REGION}:{ACCOUNT_ID}:agent/{agent_id}'


def make_target_definition(space_keys=(SPACE_KEY,)):
    """Catalog entry for the delete target."""
    return {
        'name': 'Test Agent',
        'agentId': AGENT_ID,
        'category': 'FinOps',
        'dependsOn': {'spaces': list(space_keys)},
    }


def make_deployed_agent(agent_id, cid_managed=True, space_arns=(SPACE_ARN,)):
    """DescribeAgent 'Agent' dict as returned by Agent.get."""
    description = f'agent {CID_MANAGED_MARKER}' if cid_managed else 'hand-made agent'
    return {
        'AgentId': agent_id,
        'Arn': agent_arn(agent_id),
        'Description': description,
        'Spaces': list(space_arns),
    }


def make_delete_cid(agents_catalog, deployed_agents, spaces=None):
    """Build a Cid harness with mocked qs/space/agent helpers.

    :param agents_catalog: the catalog 'agents' map (key -> entry)
    :param deployed_agents: agent_id -> DescribeAgent dict (agent.get lookup table)
    :param spaces: space_id -> space dict (space.get lookup table); default: the
                   target Space existing with CID-managed provenance
    """
    cid_obj = Cid.__new__(Cid)
    cid_obj.__dict__.clear()
    excs = make_exceptions()

    cid_obj.resources = {'agents': dict(agents_catalog), 'dashboards': {}, 'datasets': {}}
    cid_obj.base = SimpleNamespace(
        account_id=ACCOUNT_ID, region=REGION, partition=PARTITION, domain=DOMAIN,
        username='test-user', session=MagicMock(name='session'),
    )

    qs = MagicMock(name='qs')
    qs.list_dashboards.return_value = []
    cid_obj.__dict__['qs'] = qs

    if spaces is None:
        spaces = {SPACE_KEY: {
            'spaceArn': SPACE_ARN, 'name': 'Test Space',
            'description': f'space {CID_MANAGED_MARKER}',
        }}
    space = MagicMock(name='space')
    space.client.exceptions = excs
    space.client.list_tags_for_resource.return_value = {'Tags': []}
    space.get.side_effect = lambda space_id: spaces.get(space_id)
    cid_obj.__dict__['space'] = space

    agent = MagicMock(name='agent')
    agent.client.exceptions = excs
    agent.get.side_effect = lambda an_id: deployed_agents.get(an_id)
    cid_obj.__dict__['agent'] = agent
    return cid_obj


def run_delete(cid_obj, **kwargs):
    """Run the undecorated handler capturing stdout; returns (result, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = delete_agent(cid_obj, **kwargs)
    return result, buffer.getvalue()


def run_delete_raising(cid_obj, exception_type, **kwargs):
    """Run the handler expecting an exception; returns (excinfo, output)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        with pytest.raises(exception_type) as excinfo:
            delete_agent(cid_obj, **kwargs)
    return excinfo, buffer.getvalue()


def all_call_names(cid_obj):
    """Fully qualified call names recorded on the qs/space/agent helper mocks."""
    names = []
    for helper_name in ('qs', 'space', 'agent'):
        helper = cid_obj.__dict__[helper_name]
        for call in helper.mock_calls:
            if call[0]:  # skip bare __call__ entries
                names.append(f'{helper_name}.{call[0]}')
    return names


def assert_only_in_scope_calls(cid_obj):
    """Property 23 core assertion: zero out-of-scope create or delete calls.

    The delete flow may only ever delete the target Agent (``agent.delete``) and,
    when allowed, the agent's Space (``space.client.delete_space``). It must never
    issue ANY create call, and never any dashboard/dataset/knowledge-base/Research
    create or delete call.
    """
    for name in all_call_names(cid_obj):
        method = name.split('.')[-1]
        assert not any(fragment in method for fragment in FORBIDDEN_NAME_FRAGMENTS), (
            f'out-of-scope call issued by delete-agent: {name}')
        assert 'create' not in method, f'delete-agent must never create anything: {name}'
        if 'delete' in method:
            assert name in ALLOWED_DELETE_CALLS, (
                f'delete-agent issued a delete call outside its scope: {name}')


class TestConfirmationGate:
    """: deletion is gated by a yes/no parameter defaulting to 'no'."""

    def test_confirmation_prompt_defaults_to_no(self):
        """The yes/no confirmation is requested with default='no', and a
        non-confirmed answer makes no delete call."""
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()},
                           {AGENT_ID: make_deployed_agent(AGENT_ID)})
        with patch('cid.common.get_yesno_parameter', return_value=False) as yesno:
            result, output = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        yesno.assert_called_once()
        assert yesno.call_args.kwargs.get('default') == 'no'
        assert yesno.call_args.kwargs.get('param_name') == 'confirm-delete'
        assert not cid_obj.agent.delete.called
        assert 'not confirmed' in output

    def test_explicit_no_parameter_makes_no_delete_call(self):
        """With the real parameter engine and confirm-delete='no', the handler
        returns without deleting anything."""
        reset_parameters({'confirm-delete': 'no'})
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()},
                           {AGENT_ID: make_deployed_agent(AGENT_ID)})
        result, output = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        assert 'not confirmed' in output
        assert not cid_obj.agent.delete.called
        assert not cid_obj.space.client.delete_space.called


class TestMissingTargetSuccess:
    """: a missing delete target is treated as success."""

    def test_absent_agent_returns_cleanly_without_delete_call(self):
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()}, deployed_agents={})
        result, output = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        assert 'does not exist' in output
        assert not cid_obj.agent.delete.called
        assert not cid_obj.space.client.delete_space.called


class TestNoDashboardDelete:
    """: deleting an Agent never deletes any dashboard."""

    def test_successful_delete_issues_no_dashboard_delete_anywhere(self):
        reset_parameters({'confirm-delete': 'yes'})
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()},
                           {AGENT_ID: make_deployed_agent(AGENT_ID)})
        result, output = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        assert 'No dashboard was deleted' in output
        cid_obj.agent.delete.assert_called_once_with(AGENT_ID)
        # no dashboard-delete method invoked anywhere: the qs helper (which owns
        # delete_dashboard) is never touched, and the only delete call is agent.delete
        assert not cid_obj.qs.mock_calls
        names = all_call_names(cid_obj)
        assert not any('delete_dashboard' in name for name in names)
        assert [name for name in names if 'delete' in name.split('.')[-1]] == ['agent.delete']


class TestNonCidTargetRefused:
    """: a target lacking CID_Managed provenance is refused and reported."""

    def test_non_cid_agent_refused_with_report_and_no_delete_call(self):
        reset_parameters({'confirm-delete': 'yes'})
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()},
                           {AGENT_ID: make_deployed_agent(AGENT_ID, cid_managed=False)})
        excinfo, _ = run_delete_raising(cid_obj, CidError, agent_id=AGENT_ID)
        message = str(excinfo.value)
        assert 'not managed' in message
        assert AGENT_ID in message
        assert not cid_obj.agent.delete.called
        assert not cid_obj.space.client.delete_space.called

    def test_provenance_tag_alone_is_sufficient_to_proceed(self):
        """Dual-mechanism detection: the provenance tag counts even without the
        Description marker."""
        reset_parameters({'confirm-delete': 'yes'})
        target = make_deployed_agent(AGENT_ID, cid_managed=False)
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()}, {AGENT_ID: target})
        cid_obj.space.client.list_tags_for_resource.return_value = {
            'Tags': [{'Key': CID_PROVENANCE_TAG_KEY, 'Value': CID_PROVENANCE_TAG_VALUE}]}
        result, _ = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        cid_obj.agent.delete.assert_called_once_with(AGENT_ID)


class TestConflictRetryDelegation:
    """: ConflictException wait-and-retry during delete.

    The retry loop itself lives in ``Agent.delete`` (``helpers/quicksight/agent.py``)
    and is covered by ``test_agent_helper.py`` (ConflictException while the agent is
    UPDATING/CREATING is retried until the status settles, up to the 300-second
    limit). The handler's responsibility is to delegate the deletion to
    ``self.agent.delete``, which owns that retry behavior — asserted here.
    """

    def test_handler_delegates_deletion_to_the_retrying_agent_helper(self):
        reset_parameters({'confirm-delete': 'yes'})
        cid_obj = make_delete_cid({AGENT_ID: make_target_definition()},
                           {AGENT_ID: make_deployed_agent(AGENT_ID)})
        result, _ = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        # exactly one delegation to the helper that owns the ConflictException retry
        cid_obj.agent.delete.assert_called_once_with(AGENT_ID)


other_agent_configs = st.lists(
    st.tuples(
        st.booleans(),                                              # deployed?
        st.booleans(),                                              # CID-managed?
        st.sampled_from(['none', 'catalog', 'deployed', 'both']),   # references the space?
    ),
    min_size=0, max_size=4,
)


def build_catalog_and_deployed(configs):
    """Materialize other-agent configs into catalog entries + deployed agents.

    Returns (agents_catalog, deployed_agents, blocking_ids) where blocking_ids are
    the other agents that must retain the Space: deployed AND CID-managed AND
    referencing the space (via the catalog dependsOn.spaces or the deployed Spaces
    list).
    """
    agents_catalog = {AGENT_ID: make_target_definition()}
    deployed_agents = {AGENT_ID: make_deployed_agent(AGENT_ID)}
    blocking_ids = set()
    for index, (deployed, managed, reference) in enumerate(configs):
        other_id = f'other-{index}'
        entry = {'name': f'Other {index}', 'agentId': other_id, 'category': 'FinOps',
                 'dependsOn': {'spaces': [SPACE_KEY] if reference in ('catalog', 'both') else []}}
        agents_catalog[other_id] = entry
        if deployed:
            space_arns = [SPACE_ARN] if reference in ('deployed', 'both') else []
            deployed_agents[other_id] = make_deployed_agent(
                other_id, cid_managed=managed, space_arns=space_arns)
            if managed and reference != 'none':
                blocking_ids.add(other_id)
    return agents_catalog, deployed_agents, blocking_ids


@PROPERTY_SETTINGS
@given(configs=other_agent_configs)
def test_property_22_space_deleted_iff_no_other_cid_managed_agent_references_it(configs):
    """For any set of other catalog agents — each randomly deployed or not,
    CID-managed or not, referencing the Space or not — ``--delete-space`` deletes
    the Space if and only if no OTHER deployed CID-managed agent references it;
    otherwise the Space is retained and the dependency is reported."""
    reset_parameters({'confirm-delete': 'yes', 'delete-space': True})
    agents_catalog, deployed_agents, blocking_ids = build_catalog_and_deployed(configs)
    cid_obj = make_delete_cid(agents_catalog, deployed_agents)
    result, output = run_delete(cid_obj, agent_id=AGENT_ID)
    assert result == AGENT_ID
    cid_obj.agent.delete.assert_called_once_with(AGENT_ID)
    if blocking_ids:
        # retained + every blocking dependency reported
        assert not cid_obj.space.client.delete_space.called
        assert 'retained' in output
        for other_id in blocking_ids:
            assert other_id in output
    else:
        # no other deployed CID-managed agent references the space: deleted
        cid_obj.space.client.delete_space.assert_called_once_with(
            AwsAccountId=ACCOUNT_ID, SpaceId=SPACE_KEY)


@PROPERTY_SETTINGS
@given(
    target_exists=st.booleans(),
    target_managed=st.booleans(),
    confirmed=st.booleans(),
    delete_space_flag=st.booleans(),
)
def test_property_23_delete_never_issues_out_of_scope_create_or_delete_calls(
        target_exists, target_managed, confirmed, delete_space_flag):
    """Across generated delete scenarios (target exists/absent, CID/non-CID,
    confirmed/unconfirmed, delete-space on/off), the only delete calls ever made
    are ``agent.delete`` and (when allowed) ``space.client.delete_space`` — never
    any dashboard, dataset, knowledge-base, or Research create or delete call.
    A non-CID target additionally receives zero delete calls and is reported."""
    initial = {'confirm-delete': 'yes' if confirmed else 'no'}
    if delete_space_flag:
        initial['delete-space'] = True
    reset_parameters(initial)
    deployed_agents = {}
    if target_exists:
        deployed_agents[AGENT_ID] = make_deployed_agent(AGENT_ID, cid_managed=target_managed)
    cid_obj = make_delete_cid({AGENT_ID: make_target_definition()}, deployed_agents)

    if target_exists and not target_managed:
        excinfo, _ = run_delete_raising(cid_obj, CidError, agent_id=AGENT_ID)
        assert 'not managed' in str(excinfo.value)          # reported
        assert not cid_obj.agent.delete.called              # zero delete calls
        assert not cid_obj.space.client.delete_space.called
    else:
        result, _ = run_delete(cid_obj, agent_id=AGENT_ID)
        assert result == AGENT_ID
        deletion_allowed = target_exists and target_managed and confirmed
        assert cid_obj.agent.delete.called == deletion_allowed
        if not (deletion_allowed and delete_space_flag):
            assert not cid_obj.space.client.delete_space.called

    assert_only_in_scope_calls(cid_obj)


# ======================================================================
# from test_list_agents_flow.py
# ======================================================================


list_agents = Cid.list_agents.__wrapped__


def make_catalog():
    """A catalog spanning multiple categories, including a Deprecated entry."""
    return {
        'finops': {'name': 'FinOps Advisor', 'agentId': 'finops', 'category': 'FinOps'},
        'kpi': {'name': 'KPI Advisor', 'agentId': 'kpi', 'category': 'FinOps'},
        'operations': {'name': 'Ops Advisor', 'agentId': 'operations', 'category': 'Operations'},
        'legacy': {'name': 'Legacy Agent', 'agentId': 'legacy', 'category': 'Deprecated'},
    }


def make_list_cid(catalog, deployed=(), failing=(), dashboards=None, spaces=None,
                  deployed_dashboard_ids=(), dashboards_failing=False):
    """Build a Cid harness with the given catalog and mocked agent/qs helpers.

    :param catalog: the ``resources['agents']`` mapping
    :param deployed: agent ids for which agent.get returns a non-None describe
    :param failing: agent ids for which agent.get raises (live-query failure)
    :param dashboards: the ``resources['dashboards']`` mapping (key -> {'dashboardId'})
    :param spaces: the ``resources['spaces']`` mapping
    :param deployed_dashboard_ids: dashboardIds returned by the mocked ListDashboards
    :param dashboards_failing: make the mocked ListDashboards raise
    """
    cid_obj = Cid.__new__(Cid)
    cid_obj.__dict__.clear()
    cid_obj.resources = {'agents': catalog, 'dashboards': dashboards or {}, 'spaces': spaces or {}}

    agent = MagicMock(name='agent')

    def fake_get(agent_id):
        if agent_id in failing:
            raise RuntimeError(f'DescribeAgent failed for {agent_id}')
        if agent_id in deployed:
            return {'AgentId': agent_id, 'Arn': f'arn:aws:quicksight:us-east-1:123456789012:agent/{agent_id}'}
        return None

    agent.get.side_effect = fake_get
    cid_obj.__dict__['agent'] = agent

    qs = MagicMock(name='qs')
    if dashboards_failing:
        qs.list_dashboards.side_effect = RuntimeError('ListDashboards failed')
    else:
        qs.list_dashboards.return_value = [
            {'DashboardId': dashboard_id,
             'Arn': f'arn:aws:quicksight:us-east-1:123456789012:dashboard/{dashboard_id}'}
            for dashboard_id in deployed_dashboard_ids
        ]
    cid_obj.__dict__['qs'] = qs
    return cid_obj


def run_list(cid_obj, **kwargs):
    """Run the undecorated handler capturing stdout; returns (listing, output).

    ANSI color escapes are stripped from the captured output so tests can
    assert on plain text like '✓ CUDOSv5'.
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = list_agents(cid_obj, **kwargs)
    output = re.sub(r'\033\[[0-9;]*m', '', buffer.getvalue())
    return result, output


class TestCategoryGroupingAndDeployedMarking:
    """: category-grouped listing with ✓ on deployed entries."""

    def test_entries_grouped_by_category(self):
        cid_obj = make_list_cid(make_catalog())
        listing, output = run_list(cid_obj)
        assert set(listing) == {'FinOps', 'Operations'}
        assert [entry['key'] for entry in listing['FinOps']] == ['finops', 'kpi']
        assert [entry['key'] for entry in listing['Operations']] == ['operations']
        # category headers appear in the printed output
        assert 'FinOps' in output and 'Operations' in output

    def test_exactly_the_deployed_entries_are_check_marked(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('finops', 'operations'))
        listing, output = run_list(cid_obj)
        deployed = {entry['agentId'] for entries in listing.values()
                    for entry in entries if entry['deployed']}
        assert deployed == {'finops', 'operations'}
        assert f'{DEPLOYED_MARK}[finops]' in output
        assert f'{DEPLOYED_MARK}[operations]' in output
        # the non-deployed entry is listed without the check indicator
        assert '[kpi] KPI Advisor' in output
        assert f'{DEPLOYED_MARK}[kpi]' not in output

    def test_no_deployed_entries_no_check_marks(self):
        cid_obj = make_list_cid(make_catalog())
        listing, output = run_list(cid_obj)
        assert all(not entry['deployed'] for entries in listing.values() for entry in entries)
        assert DEPLOYED_MARK not in output

    def test_empty_catalog_prints_message_and_returns_empty(self):
        cid_obj = make_list_cid({})
        listing, output = run_list(cid_obj)
        assert listing == {}
        assert 'No agents found' in output
        assert not cid_obj.agent.get.called


class TestLiveStateSource:
    """: deployment status comes from live DescribeAgent per entry,
    never from ListAgents (which omits PREVIEW/FAILED agents)."""

    def test_agent_get_called_once_per_non_deprecated_entry(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('finops',))
        run_list(cid_obj)
        queried = sorted(call.args[0] for call in cid_obj.agent.get.call_args_list)
        assert queried == ['finops', 'kpi', 'operations']

    def test_status_never_relies_on_list_agents(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('finops',))
        run_list(cid_obj)
        assert not cid_obj.agent.client.list_agents.called
        # no ListAgents-shaped call anywhere on the helper either
        assert not any('list_agents' in name for name, _, _ in cid_obj.agent.mock_calls
                       if name != 'get')


class TestDeprecatedHidden:
    """: Deprecated entries are absent from output AND never described."""

    def test_deprecated_entry_absent_from_listing_and_output(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('legacy',))
        listing, output = run_list(cid_obj)
        assert 'Deprecated' not in listing
        keys = {entry['key'] for entries in listing.values() for entry in entries}
        assert 'legacy' not in keys
        assert 'legacy' not in output
        assert 'Legacy Agent' not in output

    def test_deprecated_entry_never_queried(self):
        cid_obj = make_list_cid(make_catalog())
        run_list(cid_obj)
        queried = {call.args[0] for call in cid_obj.agent.get.call_args_list}
        assert 'legacy' not in queried


class TestUnknownStatusFallback:
    """: a live-query failure marks that entry '?' and listing continues."""

    def test_failing_entry_listed_with_unknown_indicator(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('finops',), failing=('kpi',))
        listing, output = run_list(cid_obj)
        assert '?[kpi] KPI Advisor' in output
        # the failing entry is not marked deployed
        kpi = next(entry for entry in listing['FinOps'] if entry['key'] == 'kpi')
        assert kpi['deployed'] is False

    def test_remaining_entries_still_listed_after_failure(self):
        cid_obj = make_list_cid(make_catalog(), deployed=('finops',), failing=('kpi',))
        listing, output = run_list(cid_obj)
        keys = {entry['key'] for entries in listing.values() for entry in entries}
        assert keys == {'finops', 'kpi', 'operations'}
        # deployed marking on the other entries is unaffected
        assert f'{DEPLOYED_MARK}[finops]' in output
        assert '[operations] Ops Advisor' in output

    def test_all_entries_failing_all_marked_unknown_none_deployed(self):
        cid_obj = make_list_cid(make_catalog(), failing=('finops', 'kpi', 'operations'))
        listing, output = run_list(cid_obj)
        assert output.count('?[') == 3
        assert DEPLOYED_MARK not in output
        assert all(not entry['deployed'] for entries in listing.values() for entry in entries)


# ======================================================================
# --repair flag plumbing
# ======================================================================


class TestRepairFlag:
    """--repair (update-agent) rewrites the Space links of the deployed agent
    through Agent.repair_space_associations; create-agent never repairs."""

    @staticmethod
    def existing_cid_agent():
        """A deployed, CID-managed agent (Description carries the provenance marker)."""
        return {
            'AgentId': AGENT_ID, 'Arn': AGENT_ARN, 'AgentStatus': 'ACTIVE',
            'Name': 'Test Agent', 'Description': f'A test agent {CID_MANAGED_MARKER}',
            'Spaces': [SPACE_ARN],
        }

    def test_repair_flag_repairs_the_deployed_agent(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',), existing_agent=self.existing_cid_agent())
        reset_parameters({'repair': True, 'confirm-update': 'yes'})

        run_update(cid_obj, agent_id=AGENT_ID)

        cid_obj.agent.repair_space_associations.assert_called_once_with(AGENT_ID)

    def test_create_agent_never_repairs(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',), existing_agent=self.existing_cid_agent())
        reset_parameters({'repair': True, 'update': 'yes'})

        run_create(cid_obj, agent_id=AGENT_ID)

        cid_obj.agent.repair_space_associations.assert_not_called()

    def test_no_repair_without_the_flag(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',), existing_agent=self.existing_cid_agent())
        reset_parameters({'confirm-update': 'yes'})

        run_update(cid_obj, agent_id=AGENT_ID)

        cid_obj.agent.repair_space_associations.assert_not_called()


# ======================================================================
# create-agent existing-agent gate + update-agent command
# ======================================================================


class TestCreateAgentGate:
    """create-agent on an already-deployed agent asks before updating; without
    consent it exits with guidance and makes no mutating call."""

    def test_declined_update_exits_with_guidance(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',),
                                  existing_agent=TestRepairFlag.existing_cid_agent())
        reset_parameters({'update': 'no'})

        result, output = run_create(cid_obj, agent_id=AGENT_ID)

        assert result == AGENT_ID
        assert 'update-agent' in output
        for method in AGENT_MUTATING_METHODS:
            assert not getattr(cid_obj.agent, method).called
        assert not cid_obj.space.create_or_update.called
        assert not cid_obj.space.update_resources.called

    def test_update_yes_proceeds_into_the_update_path(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',),
                                  existing_agent=TestRepairFlag.existing_cid_agent())
        reset_parameters({'update': 'yes'})

        result, _ = run_create(cid_obj, agent_id=AGENT_ID)

        assert result == AGENT_ID
        cid_obj.agent.create_or_update.assert_called_once()

    def test_unattended_without_update_parameter_raises_with_guidance(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',),
                                  existing_agent=TestRepairFlag.existing_cid_agent())

        with patch('cid.common.isatty', return_value=False):
            excinfo, _ = run_create_raising(cid_obj, CidError, agent_id=AGENT_ID)

        assert '--update yes' in str(excinfo.value)

    def test_fresh_create_is_unaffected_by_the_gate(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',))

        result, output = run_create(cid_obj, agent_id=AGENT_ID)

        assert result == AGENT_ID
        cid_obj.agent.create_or_update.assert_called_once()
        assert 'created' in output


class TestUpdateAgentCommand:
    """update-agent requires a deployed agent, shows drift, and passes
    sync-spaces through to the helper."""

    def test_missing_agent_raises_with_create_guidance(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',))

        excinfo, _ = run_update_raising(cid_obj, CidError, agent_id=AGENT_ID)

        assert 'create-agent' in str(excinfo.value)
        for method in AGENT_MUTATING_METHODS:
            assert not getattr(cid_obj.agent, method).called

    def test_drift_is_shown_and_update_applies_on_consent(self):
        definition = make_definition(required=('dash',))
        deployed = dict(TestRepairFlag.existing_cid_agent(), WelcomeMessage='old welcome')
        cid_obj = make_create_cid(definition, present=('dash',), existing_agent=deployed)
        reset_parameters({'confirm-update': 'yes'})

        result, output = run_update(cid_obj, agent_id=AGENT_ID)

        assert result == AGENT_ID
        assert 'welcomeMessage' in output
        cid_obj.agent.create_or_update.assert_called_once()

    def test_declined_drift_confirm_makes_no_mutating_call(self):
        definition = make_definition(required=('dash',))
        deployed = dict(TestRepairFlag.existing_cid_agent(), WelcomeMessage='old welcome')
        cid_obj = make_create_cid(definition, present=('dash',), existing_agent=deployed)
        reset_parameters({'confirm-update': 'no'})

        result, output = run_update(cid_obj, agent_id=AGENT_ID)

        assert result == AGENT_ID
        assert 'No changes made' in output
        cid_obj.agent.create_or_update.assert_not_called()

    def test_sync_spaces_parameter_reaches_the_helper(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',),
                                  existing_agent=TestRepairFlag.existing_cid_agent())
        reset_parameters({'confirm-update': 'yes', 'sync-spaces': True})

        run_update(cid_obj, agent_id=AGENT_ID)

        assert cid_obj.agent.create_or_update.call_args.kwargs.get('sync_spaces') is True

    def test_default_is_additive_spaces(self):
        definition = make_definition(required=('dash',))
        cid_obj = make_create_cid(definition, present=('dash',),
                                  existing_agent=TestRepairFlag.existing_cid_agent())
        reset_parameters({'confirm-update': 'yes'})

        run_update(cid_obj, agent_id=AGENT_ID)

        assert cid_obj.agent.create_or_update.call_args.kwargs.get('sync_spaces') is False


class TestListAgentsDependencyAnnotations:
    """list-agents annotates each entry with its dependsOn dashboards and their
    deployment state, using the same key-resolution the create-agent pre-flight uses."""

    DASHBOARDS = {
        'CUDOSv5': {'dashboardId': 'cudos-v5'},
        'KPI': {'dashboardId': 'kpi_dashboard'},
        'Trends': {'dashboardId': 'trends-dashboard'},
    }

    def _catalog(self):
        return {
            'finops': {'name': 'FinOps Advisor', 'agentId': 'finops', 'category': 'FinOps',
                       'dependsOn': {'dashboards': ['CUDOSv5', 'KPI'],
                                     'optionalDashboards': ['Trends']}},
        }

    def test_dependency_line_marks_deployed_and_missing(self):
        cid_obj = make_list_cid(self._catalog(), dashboards=self.DASHBOARDS,
                                deployed_dashboard_ids=('cudos-v5',))
        _, output = run_list(cid_obj)
        assert '✓ CUDOSv5' in output
        assert '✗ KPI' in output
        assert 'optional:' in output
        assert '✗ Trends' in output

    def test_space_declared_dashboards_are_included(self):
        catalog = {'finops': {'name': 'FinOps Advisor', 'agentId': 'finops', 'category': 'FinOps',
                              'dependsOn': {'spaces': ['shared'], 'dashboards': ['KPI']}}}
        spaces = {'shared': {'dependsOn': {'dashboards': ['CUDOSv5']}}}
        cid_obj = make_list_cid(catalog, dashboards=self.DASHBOARDS, spaces=spaces,
                                deployed_dashboard_ids=('cudos-v5',))
        _, output = run_list(cid_obj)
        assert '✓ CUDOSv5' in output   # merged in from the Space's dependsOn
        assert '✗ KPI' in output

    def test_listing_continues_without_annotations_when_listdashboards_fails(self):
        cid_obj = make_list_cid(self._catalog(), dashboards=self.DASHBOARDS,
                                dashboards_failing=True)
        listing, output = run_list(cid_obj)
        assert 'required:' not in output   # annotations skipped
        assert '[finops] FinOps Advisor' in output   # listing still rendered
        assert listing

    def test_no_dependency_line_for_agents_without_dependencies(self):
        cid_obj = make_list_cid({'plain': {'name': 'Plain', 'agentId': 'plain', 'category': 'FinOps'}})
        _, output = run_list(cid_obj)
        assert 'required:' not in output


class TestAgentPickerOptions:
    """_agent_picker_options: name-first labels, id only on display-name collision,
    and non-selectable dependency annotation lines when dashboard state is known."""

    DASHBOARDS = {'CUDOSv5': {'dashboardId': 'cudos-v5'}, 'Trends': {'dashboardId': 'trends-dashboard'}}

    def _make_cid(self, catalog):
        cid_obj = Cid.__new__(Cid)
        cid_obj.__dict__.clear()
        cid_obj.resources = {'agents': catalog, 'dashboards': self.DASHBOARDS, 'spaces': {}}
        return cid_obj

    def _selectable(self, options):
        return {label: value for label, value in options.items() if not isinstance(value, Separator)}

    def _separators(self, options):
        return [str(value) for value in options.values() if isinstance(value, Separator)]

    def test_labels_are_name_first_without_agent_id(self):
        catalog = {'finops': {'name': 'FinOps Advisor', 'agentId': 'cid-finops', 'category': 'FinOps'}}
        cid_obj = self._make_cid(catalog)
        options = cid_obj._agent_picker_options(build_agent_listing(catalog, ()))
        labels = list(self._selectable(options))
        assert labels == ['FinOps Advisor']   # no id in the label

    def test_duplicate_display_names_fall_back_to_appending_the_id(self):
        catalog = {
            'one': {'name': 'Advisor', 'agentId': 'agent-one', 'category': 'FinOps'},
            'two': {'name': 'Advisor', 'agentId': 'agent-two', 'category': 'FinOps'},
        }
        cid_obj = self._make_cid(catalog)
        options = cid_obj._agent_picker_options(build_agent_listing(catalog, ()))
        selectable = self._selectable(options)
        assert len(selectable) == 2   # no silent collision
        assert any('agent-one' in label for label in selectable)
        assert any('agent-two' in label for label in selectable)

    def test_dependency_annotation_lines_are_non_selectable(self):
        catalog = {'finops': {'name': 'FinOps Advisor', 'agentId': 'cid-finops', 'category': 'FinOps',
                              'dependsOn': {'dashboards': ['CUDOSv5'], 'optionalDashboards': ['Trends']}}}
        cid_obj = self._make_cid(catalog)
        deployed = {'cudos-v5': 'arn:aws:quicksight:us-east-1:123456789012:dashboard/cudos-v5'}
        options = cid_obj._agent_picker_options(build_agent_listing(catalog, ()), catalog, deployed)
        separators = self._separators(options)
        dependency_lines = [line for line in separators if 'required:' in line]
        assert len(dependency_lines) == 1
        assert '✓ CUDOSv5' in dependency_lines[0]
        assert '✗ Trends' in dependency_lines[0]
        assert '<GREEN>' not in dependency_lines[0]   # picker lines carry no color tags
        # the agent row itself is still the only selectable entry
        assert list(self._selectable(options).values()) == ['finops']

    def test_no_dependency_lines_when_dashboard_state_unknown(self):
        catalog = {'finops': {'name': 'FinOps Advisor', 'agentId': 'cid-finops', 'category': 'FinOps',
                              'dependsOn': {'dashboards': ['CUDOSv5']}}}
        cid_obj = self._make_cid(catalog)
        options = cid_obj._agent_picker_options(build_agent_listing(catalog, ()), catalog, None)
        assert not [line for line in self._separators(options) if 'required:' in line]


class TestDependencySummaryWrapping:
    """_dependency_summary_lines wraps to the given width: one line when it fits,
    one group per line when not, key continuation aligned under the first key."""

    def test_single_line_when_it_fits(self):
        lines = Cid._dependency_summary_lines(['a', 'b'], ['c'], {'a'}, color=False, width=120)
        assert lines == ['     required: ✓ a  ✗ b    optional: ✗ c']

    def test_groups_split_when_combined_line_does_not_fit(self):
        lines = Cid._dependency_summary_lines(['dash-one', 'dash-two'], ['dash-three'], set(),
                                              color=False, width=40)
        assert len(lines) == 2
        assert lines[0].startswith('     required: ')
        assert lines[1].startswith('     optional: ')

    def test_keys_wrap_with_aligned_continuation(self):
        keys = [f'dashboard-{i}' for i in range(6)]
        lines = Cid._dependency_summary_lines(keys, [], set(), color=False, width=50)
        assert len(lines) > 1
        prefix = '     required: '
        assert lines[0].startswith(prefix)
        for continuation in lines[1:]:
            assert continuation.startswith(' ' * len(prefix))   # aligned under the first key
            assert len(continuation) <= 50

    def test_color_tags_do_not_count_toward_width(self):
        plain = Cid._dependency_summary_lines(['a', 'b'], [], {'a'}, color=False, width=30)
        colored = Cid._dependency_summary_lines(['a', 'b'], [], {'a'}, color=True, width=30)
        assert len(plain) == len(colored)   # same wrapping decisions


class TestDeployedAgentIdsEnumeration:
    """_deployed_agent_ids extracts ids from the real ListAgents response shape
    ('AgentSummaries', verified live) and follows NextToken pagination."""

    def _make_cid_with_pages(self, pages):
        cid_obj = Cid.__new__(Cid)
        cid_obj.__dict__.clear()
        cid_obj.base = SimpleNamespace(account_id=ACCOUNT_ID)
        agent = MagicMock(name='agent')
        agent.client.list_agents.side_effect = pages
        cid_obj.__dict__['agent'] = agent
        return cid_obj

    def test_extracts_ids_from_agent_summaries(self):
        cid_obj = self._make_cid_with_pages([
            {'AgentSummaries': [{'AgentId': 'cid-finops-advisor'}, {'AgentId': 'SYSTEM'}]},
        ])
        assert cid_obj._deployed_agent_ids() == {'cid-finops-advisor', 'SYSTEM'}

    def test_follows_next_token_pagination(self):
        cid_obj = self._make_cid_with_pages([
            {'AgentSummaries': [{'AgentId': 'a1'}], 'NextToken': 'page2'},
            {'AgentSummaries': [{'AgentId': 'a2'}]},
        ])
        assert cid_obj._deployed_agent_ids() == {'a1', 'a2'}

    def test_list_agents_failure_yields_empty_set(self):
        cid_obj = self._make_cid_with_pages(RuntimeError('AccessDenied'))
        assert cid_obj._deployed_agent_ids() == set()
