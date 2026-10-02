""" Tests for the Agent and Space AWS helpers (cid.helpers.quicksight.agent / .space).
"""

from unittest.mock import MagicMock, patch
import pytest
from hypothesis import given, settings, strategies as st
from cid.exceptions import CidError
from cid.helpers.quicksight.agent import Agent, AGENT_OWNER_ACTIONS
from cid.helpers.quicksight.agent_logic import PERSONA_API_FIELDS, PERSONA_FIELDS
import logging
from cid.helpers.quicksight.agent_logic import CID_MANAGED_MARKER
from unittest.mock import MagicMock
from cid.base import CidBase
from cid.helpers.quicksight.agent_logic import (
    CID_MANAGED_MARKER,
    CID_PROVENANCE_TAG_KEY,
    CID_PROVENANCE_TAG_VALUE,
)
from cid.helpers.quicksight.space import SPACE_OWNER_ACTIONS, Space


# ======================================================================
# from test_agent_helper.py
# ======================================================================


ACCOUNT_ID = '123456789012'


class ResourceNotFoundException(Exception):
    pass


class ResourceExistsException(Exception):
    pass


class ConflictException(Exception):
    pass


class AccessDeniedException(Exception):
    pass


class ClientError(Exception):
    pass


def make_agent_helper():
    """Build an Agent helper over a MagicMock quicksight client (no boto3)."""
    client = MagicMock()
    client.exceptions.ResourceNotFoundException = ResourceNotFoundException
    client.exceptions.ResourceExistsException = ResourceExistsException
    client.exceptions.ConflictException = ConflictException
    client.exceptions.AccessDeniedException = AccessDeniedException
    client.exceptions.ClientError = ClientError
    session = MagicMock()
    session.client.return_value = client
    helper = Agent(session=session)
    helper.awsIdentity = {'Account': ACCOUNT_ID}
    return helper, client


def persona_camel(suffix=''):
    """A full 5-field camelCase catalog persona."""
    return {field: f'{field}-value{suffix}' for field in PERSONA_FIELDS}


def persona_pascal_from(camel):
    """Map a camelCase persona to the PascalCase API read shape (CustomPromptInterface)."""
    lowered = {key.lower(): str(value) for key, value in camel.items()}
    return {field: lowered[field.lower()] for field in PERSONA_API_FIELDS if field.lower() in lowered}


def deployed_agent_matching(definition, space_arns):
    """The DescribeAgent read shape exactly matching the desired definition (no drift)."""
    return {
        'Arn': f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:agent/{definition["agentId"]}',
        'AgentId': definition['agentId'],
        'Name': definition['name'],
        'CustomPromptInterface': dict(
            persona_pascal_from(definition.get('persona') or {}),
            PromptSummary='server-derived summary',
        ),
        'StarterPrompts': list(definition.get('starterPrompts') or []),
        'WelcomeMessage': definition.get('welcomeMessage'),
        'AgentLifecycle': definition.get('lifecycle'),
        'Spaces': sorted(space_arns),
        'ActionConnectors': [],
        'AgentStatus': 'ACTIVE',
    }


def base_definition():
    return {
        'agentId': 'cid-finops-advisor',
        'name': 'CID FinOps Advisor',
        'description': 'A test agent',
        'persona': persona_camel(),
        'starterPrompts': ['What did I spend last month?', 'Top 5 services by cost'],
        'welcomeMessage': 'Welcome to the FinOps advisor.',
        'lifecycle': 'PUBLISHED',
    }


SPACE_ARNS = [f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/cid-space']


def wire_create_lifecycle(client, created_agent):
    """describe_agent raises NotFound until create_agent has been called, then returns
    ``created_agent`` (the settled, freshly created agent with NO Spaces attached).

    Mirrors the lifecycle the post-create Space attach depends on: the agent is
    absent before CreateAgent and describable (settled) afterwards.
    """
    def describe(**kwargs):
        if client.create_agent.call_count:
            return {'Agent': created_agent}
        raise ResourceNotFoundException('not found')
    client.describe_agent.side_effect = describe


def created_agent_shape(definition, arn='arn:new-agent', status='ACTIVE'):
    """The DescribeAgent read shape of a freshly created, settled agent (no Spaces)."""
    return {
        'Arn': arn,
        'AgentId': definition.get('agentId'),
        'Name': definition.get('name'),
        'Description': definition.get('description'),
        'StarterPrompts': list(definition.get('starterPrompts') or []),
        'WelcomeMessage': definition.get('welcomeMessage'),
        'CustomPromptInterface': persona_pascal_from(definition.get('persona') or {}),
        'AgentStatus': status,
        'Spaces': [],
    }


class TestCreateOrUpdateBranches:
    """No-op vs update vs create branches."""

    def test_noop_when_configuration_matches(self):
        """Matching desired/deployed config makes no mutating API call."""
        helper, client = make_agent_helper()
        definition = base_definition()
        client.describe_agent.return_value = {'Agent': deployed_agent_matching(definition, SPACE_ARNS)}

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'unchanged'
        assert result['agentId'] == definition['agentId']
        client.update_agent.assert_not_called()
        client.create_agent.assert_not_called()

    def test_update_when_configuration_drifts(self):
        """Drifted config triggers exactly one UpdateAgent call."""
        helper, client = make_agent_helper()
        definition = base_definition()
        deployed = deployed_agent_matching(definition, SPACE_ARNS)
        deployed['WelcomeMessage'] = 'An old welcome message'
        client.describe_agent.return_value = {'Agent': deployed}

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'updated'
        client.update_agent.assert_called_once()
        client.create_agent.assert_not_called()
        payload = client.update_agent.call_args.kwargs
        assert payload['WelcomeMessage'] == definition['welcomeMessage']

    def test_create_when_agent_absent(self):
        """A missing agent (DescribeAgent NotFound) triggers CreateAgent."""
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'created'
        assert result['arn'] == 'arn:new-agent'
        client.create_agent.assert_called_once()
        # the ONLY post-create mutation is the Space attach (UpdateAgent SpacesToAdd)
        client.update_agent.assert_called_once()
        assert client.update_agent.call_args.kwargs['SpacesToAdd'] == sorted(SPACE_ARNS)


class TestUpdateAlwaysIncludesName:
    """Name is included on EVERY UpdateAgent call."""

    def test_name_present_on_update(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        deployed = deployed_agent_matching(definition, SPACE_ARNS)
        deployed['WelcomeMessage'] = 'drifted'  # welcome-message drift only
        client.describe_agent.return_value = {'Agent': deployed}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.update_agent.call_args.kwargs
        assert payload['Name'] == definition['name']

    def test_deployed_name_carried_when_definition_has_none(self):
        """When the definition carries no name, the deployed Name is carried through."""
        helper, client = make_agent_helper()
        definition = base_definition()
        definition['name'] = None
        deployed = deployed_agent_matching(base_definition(), SPACE_ARNS)
        deployed['WelcomeMessage'] = 'drifted'
        client.describe_agent.return_value = {'Agent': deployed}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.update_agent.call_args.kwargs
        assert payload['Name'] == 'CID FinOps Advisor'


class TestResourceExistsTreatedAsSuccess:
    """ResourceExistsException during create is success."""

    def test_resource_exists_on_create_is_success(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        existing_arn = 'arn:aws:quicksight:us-east-1:123456789012:agent/cid-finops-advisor'
        # First describe: absent (drives the create path). After the racing create
        # fails with ResourceExistsException, the re-describes find the agent
        # (arn recovery + the post-create Space attach settle/describe reads).
        racing_agent = dict(created_agent_shape(definition, arn=existing_arn), Spaces=list(SPACE_ARNS))
        client.describe_agent.side_effect = [
            ResourceNotFoundException('not found'),
        ] + [{'Agent': racing_agent}] * 3
        client.create_agent.side_effect = ResourceExistsException('already exists')

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'created'
        assert result['arn'] == existing_arn
        # the racing agent already carries the Space: nothing to add, no mutation
        client.update_agent.assert_not_called()


class TestWaitActive:
    """wait_active polling behavior."""

    def test_creating_then_active_returns_agent(self):
        """CREATING -> CREATING -> ACTIVE sequence returns the ACTIVE agent."""
        helper, client = make_agent_helper()
        client.describe_agent.side_effect = [
            {'Agent': {'AgentStatus': 'CREATING'}},
            {'Agent': {'AgentStatus': 'CREATING'}},
            {'Agent': {'AgentStatus': 'ACTIVE', 'Arn': 'arn:agent'}},
        ]
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            agent = helper.wait_active('my-agent')
        assert agent['AgentStatus'] == 'ACTIVE'
        assert mock_sleep.call_count == 2

    def test_timeout_raises_ciderror_with_agent_id_and_last_status(self):
        """Timeout raises CidError naming the agent and its last status."""
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': {'AgentStatus': 'CREATING'}}
        with patch('cid.helpers.quicksight.agent.time.sleep'):
            with pytest.raises(CidError) as exc_info:
                helper.wait_active('my-agent', timeout=300, interval=5)
        message = str(exc_info.value)
        assert 'my-agent' in message
        assert 'CREATING' in message
        assert '300' in message

    def test_failed_status_fails_fast_with_error_message(self):
        """FAILED raises immediately with ErrorMessage, not via timeout."""
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {
            'Agent': {'AgentStatus': 'FAILED', 'ErrorMessage': 'model access denied'},
        }
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            with pytest.raises(CidError) as exc_info:
                helper.wait_active('my-agent', timeout=300, interval=5)
        message = str(exc_info.value)
        assert 'my-agent' in message
        assert 'model access denied' in message
        # Fail fast: raised on the FIRST poll, without sleeping through the timeout.
        assert mock_sleep.call_count == 0
        assert client.describe_agent.call_count == 1


class TestGrantOwner:
    """AGENT_OWNER_ACTIONS granted as one single set."""

    def test_grant_owner_sends_the_five_actions_in_one_call(self):
        helper, client = make_agent_helper()
        principal = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/admin'

        helper.grant_owner('my-agent', principal)

        client.update_agent_permissions.assert_called_once()
        payload = client.update_agent_permissions.call_args.kwargs
        assert payload['AgentId'] == 'my-agent'
        grants = payload['GrantPermissions']
        assert len(grants) == 1  # single-set grant, never piecemeal
        assert grants[0]['Principal'] == principal
        assert grants[0]['Actions'] == AGENT_OWNER_ACTIONS
        assert len(AGENT_OWNER_ACTIONS) == 5


class TestDelete:
    """delete tolerates missing targets and retries through ConflictException."""

    def test_delete_success(self):
        helper, client = make_agent_helper()
        helper.delete('my-agent')
        client.delete_agent.assert_called_once_with(AwsAccountId=ACCOUNT_ID, AgentId='my-agent')

    def test_delete_missing_target_is_success(self):
        helper, client = make_agent_helper()
        client.delete_agent.side_effect = ResourceNotFoundException('gone')
        helper.delete('my-agent')  # must not raise

    def test_conflict_settles_then_delete_succeeds(self):
        """ConflictException while UPDATING retries until the status settles."""
        helper, client = make_agent_helper()
        client.delete_agent.side_effect = [
            ConflictException('agent is UPDATING'),
            ConflictException('agent is UPDATING'),
            None,
        ]
        client.describe_agent.return_value = {'Agent': {'AgentStatus': 'UPDATING'}}
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            helper.delete('my-agent', timeout=300, interval=5)
        assert client.delete_agent.call_count == 3
        assert mock_sleep.call_count == 2

    def test_conflict_never_settles_raises_ciderror_at_300s_bound(self):
        """A conflict that never settles raises CidError once 300s elapse."""
        helper, client = make_agent_helper()
        client.delete_agent.side_effect = ConflictException('agent is UPDATING')
        client.describe_agent.return_value = {'Agent': {'AgentStatus': 'UPDATING'}}
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            with pytest.raises(CidError) as exc_info:
                helper.delete('my-agent', timeout=300, interval=5)
        assert 'my-agent' in str(exc_info.value)
        assert '300' in str(exc_info.value)
        # 300s bound at 5s intervals: 60 sleeps, raising on the attempt where elapsed >= 300.
        assert mock_sleep.call_count == 300 // 5


class TestDescribeBasedExistence:
    """Existence detection uses describe_agent, never list_agents."""

    def test_get_uses_describe_agent_not_list_agents(self):
        """get() must work for agents that ListAgents omits (PREVIEW/FAILED states)."""
        helper, client = make_agent_helper()
        # Simulate an agent invisible to ListAgents (e.g. FAILED state) but
        # fully describable by id.
        client.list_agents.return_value = {'AgentSummaries': []}
        client.describe_agent.return_value = {'Agent': {'AgentId': 'my-agent', 'AgentStatus': 'FAILED'}}

        agent = helper.get('my-agent')

        assert agent == {'AgentId': 'my-agent', 'AgentStatus': 'FAILED'}
        client.describe_agent.assert_called_once_with(AwsAccountId=ACCOUNT_ID, AgentId='my-agent')
        client.list_agents.assert_not_called()

    def test_get_returns_none_when_absent(self):
        helper, client = make_agent_helper()
        client.describe_agent.side_effect = ResourceNotFoundException('not found')
        assert helper.get('my-agent') is None
        client.list_agents.assert_not_called()


_names = st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -', min_size=1, max_size=50).filter(lambda s: s.strip())


_ids = st.text(alphabet='abcdefghijklmnopqrstuvwxyz0123456789-', min_size=1, max_size=20)


_persona_values = st.text(alphabet='abcdefghijklmnopqrstuvwxyz .,', min_size=5, max_size=40)


_personas = st.fixed_dictionaries({field: _persona_values for field in PERSONA_FIELDS})


_starter_prompt_lists = st.lists(st.text(alphabet='abcdefghijklmnopqrstuvwxyz ?', min_size=1, max_size=60), min_size=0, max_size=3)


_welcomes = st.text(alphabet='abcdefghijklmnopqrstuvwxyz .!', min_size=1, max_size=100)


_lifecycles = st.sampled_from(['PUBLISHED', 'DRAFT'])


_space_arn_lists = st.lists(
    _ids.map(lambda s: f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/{s}'),
    min_size=0, max_size=3, unique=True,
)


def _expected_new_prompt(persona):
    """The exact PascalCase NewPrompt write shape expected for a camelCase persona."""
    return {api_field: str(persona[catalog_field])
            for api_field, catalog_field in zip(PERSONA_API_FIELDS, PERSONA_FIELDS)}


@settings(max_examples=100, deadline=None)
@given(
    agent_id=_ids,
    name=_names,
    persona=_personas,
    starter_prompts=_starter_prompt_lists,
    welcome=_welcomes,
    space_arns=_space_arn_lists,
    drift_field=st.sampled_from(['persona', 'starter_prompts', 'welcome', 'name']),
)
def test_property_16_update_full_replacement_and_name_always_sent(
        agent_id, name, persona, starter_prompts, welcome, space_arns, drift_field):
    """Property 16: any update payload carries the exact desired persona, starter
    prompts, and welcome message as full replacements (never merged with the
    deployed values) and always includes a non-empty Name.

    **Validates: Requirements 7.5, 7.6**
    """
    definition = {
        'agentId': agent_id,
        'name': name,
        'persona': persona,
        'starterPrompts': starter_prompts,
        'welcomeMessage': welcome,
    }
    deployed = deployed_agent_matching(definition, space_arns)
    # Inject guaranteed drift on one field so the update branch is always taken.
    if drift_field == 'persona':
        deployed['CustomPromptInterface']['Identity'] = deployed['CustomPromptInterface']['Identity'] + 'X'
    elif drift_field == 'starter_prompts':
        deployed['StarterPrompts'] = deployed['StarterPrompts'] + ['a stale deployed prompt']
    elif drift_field == 'welcome':
        deployed['WelcomeMessage'] = deployed['WelcomeMessage'] + 'X'
    else:  # name
        deployed['Name'] = deployed['Name'] + 'X'

    helper, client = make_agent_helper()
    client.describe_agent.return_value = {'Agent': deployed}

    result = helper.create_or_update(definition, space_arns)

    assert result['action'] == 'updated'
    client.update_agent.assert_called_once()
    payload = client.update_agent.call_args.kwargs
    # Name is always included and non-empty
    assert payload.get('Name') == name
    assert str(payload['Name']).strip()
    # Full-replacement fields carry the complete desired values, not deltas
    assert payload['CustomPromptInput'] == {'NewPrompt': _expected_new_prompt(persona)}
    assert payload['StarterPrompts'] == list(starter_prompts)
    assert payload['WelcomeMessage'] == welcome


@settings(max_examples=100, deadline=None)
@given(
    agent_id=_ids,
    name=_names,
    persona=_personas,
    starter_prompts=_starter_prompt_lists,
    welcome=_welcomes,
    lifecycle=_lifecycles,
    space_arns=_space_arn_lists,
)
def test_property_18_create_agent_payload_fidelity(
        agent_id, name, persona, starter_prompts, welcome, lifecycle, space_arns):
    """Property 18: for any valid definition with the agent absent, the CreateAgent
    request contains all five persona fields (PascalCase NewPrompt), every starter
    prompt, the welcome message, every attached space ARN, and the lifecycle value.

    **Validates: Requirements 6.10**
    """
    definition = {
        'agentId': agent_id,
        'name': name,
        'persona': persona,
        'starterPrompts': starter_prompts,
        'welcomeMessage': welcome,
        'lifecycle': lifecycle,
    }
    helper, client = make_agent_helper()
    wire_create_lifecycle(client, created_agent_shape(definition))
    client.create_agent.return_value = {'Arn': 'arn:new-agent'}

    result = helper.create_or_update(definition, space_arns)

    assert result['action'] == 'created'
    client.create_agent.assert_called_once()
    payload = client.create_agent.call_args.kwargs
    assert payload['AgentId'] == agent_id
    assert payload['Name'] == name
    # All 5 persona fields, mapped to the PascalCase NewPrompt write shape
    new_prompt = payload['CustomPromptInput']['NewPrompt']
    assert new_prompt == _expected_new_prompt(persona)
    assert set(new_prompt) == set(PERSONA_API_FIELDS)
    # Every starter prompt and the welcome message
    assert payload['StarterPrompts'] == list(starter_prompts)
    assert payload['WelcomeMessage'] == welcome
    # Lifecycle passed through
    assert payload['AgentLifecycle'] == lifecycle
    # Spaces are not passed inside CreateAgent; they are attached afterwards via a
    # single UpdateAgent SpacesToAdd against the settled agent, carrying Name through
    # and never sending the create-only AgentLifecycle.
    assert 'Spaces' not in payload
    if space_arns:
        client.update_agent.assert_called_once()
        attach = client.update_agent.call_args.kwargs
        assert attach['SpacesToAdd'] == sorted(space_arns)
        assert attach['AgentId'] == agent_id
        assert attach.get('Name') == name
        assert 'AgentLifecycle' not in attach
        assert 'SpacesToRemove' not in attach
    else:
        client.update_agent.assert_not_called()


@settings(max_examples=100, deadline=None)
@given(
    agent_id=st.text(min_size=1, max_size=40),
    principal=st.text(min_size=1, max_size=80),
)
def test_property_17_agent_owner_grant_exact_action_set(agent_id, principal):
    """Property 17 (Agent portion): for any agent id and principal ARN, grant_owner
    calls update_agent_permissions exactly once with a single GrantPermissions entry
    carrying exactly the 5 AGENT_OWNER_ACTIONS - no more and no fewer.

    **Validates: Requirements 6.9, 6.11**
    """
    helper, client = make_agent_helper()

    helper.grant_owner(agent_id, principal)

    client.update_agent_permissions.assert_called_once()
    payload = client.update_agent_permissions.call_args.kwargs
    assert payload['AwsAccountId'] == ACCOUNT_ID
    assert payload['AgentId'] == agent_id
    grants = payload['GrantPermissions']
    assert len(grants) == 1  # one single-set grant, never piecemeal
    assert grants[0]['Principal'] == principal
    actions = grants[0]['Actions']
    assert actions == AGENT_OWNER_ACTIONS
    assert len(actions) == 5
    assert set(actions) == {
        'quicksight:DescribeAgent',
        'quicksight:UpdateAgent',
        'quicksight:DeleteAgent',
        'quicksight:DescribeAgentPermissions',
        'quicksight:UpdateAgentPermissions',
    }


class TestCreateBakesProvenanceMarker:
    """_create writes the CID-managed marker into the Description at CreateAgent
    time, so no post-create UpdateAgent marker write is ever needed."""

    def test_create_appends_marker_to_description(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.create_agent.call_args.kwargs
        assert payload['Description'] == f'{definition["description"]} {CID_MANAGED_MARKER}'

    def test_create_without_description_sends_marker_only(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        definition['description'] = None
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.create_agent.call_args.kwargs
        assert payload['Description'] == CID_MANAGED_MARKER

    def test_create_does_not_duplicate_an_existing_marker(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        definition['description'] = f'A test agent {CID_MANAGED_MARKER}'
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.create_agent.call_args.kwargs
        assert payload['Description'].count(CID_MANAGED_MARKER) == 1


class TestSpacesAttachedAfterCreateNotAtCreate:
    """The Space attach happens via UpdateAgent SpacesToAdd against the SETTLED
    agent, not inside CreateAgent. Creation and knowledge-source association are kept
    as separate, independently retryable steps."""

    def test_create_payload_never_carries_spaces(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, SPACE_ARNS)

        assert 'Spaces' not in client.create_agent.call_args.kwargs

    def test_attach_waits_for_settle_before_updating(self):
        """The attach UpdateAgent fires only after the publish workflow settles
        (CREATING/UPDATING agents are never mutated)."""
        helper, client = make_agent_helper()
        definition = base_definition()
        creating = created_agent_shape(definition, status='CREATING')
        updating = created_agent_shape(definition, status='UPDATING')
        active = created_agent_shape(definition, status='ACTIVE')

        def describe(**kwargs):
            if not client.create_agent.call_count:
                raise ResourceNotFoundException('not found')
            calls_after_create = describe.post_create_calls = getattr(describe, 'post_create_calls', 0) + 1
            return {'Agent': [creating, updating, active][min(calls_after_create - 1, 2)]}
        client.describe_agent.side_effect = describe
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            helper.create_or_update(definition, SPACE_ARNS)

        client.update_agent.assert_called_once()
        attach = client.update_agent.call_args.kwargs
        assert attach['SpacesToAdd'] == sorted(SPACE_ARNS)
        # two transitional polls (CREATING, UPDATING) before the settled attach
        assert mock_sleep.call_count == 2

    def test_attach_carries_full_deployed_config_and_name(self):
        """Full-replacement semantics: the attach UpdateAgent carries the deployed
        Description/StarterPrompts/WelcomeMessage/persona through and includes Name;
        AgentLifecycle (create-only) is never sent."""
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, SPACE_ARNS)

        attach = client.update_agent.call_args.kwargs
        assert attach['Name'] == definition['name']
        assert attach['Description'] == definition['description']
        assert attach['StarterPrompts'] == definition['starterPrompts']
        assert attach['WelcomeMessage'] == definition['welcomeMessage']
        assert attach['CustomPromptInput'] == {'NewPrompt': _expected_new_prompt(definition['persona'])}
        assert 'AgentLifecycle' not in attach

    def test_no_spaces_means_no_post_create_update(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}

        helper.create_or_update(definition, [])

        client.create_agent.assert_called_once()
        client.update_agent.assert_not_called()

    def test_attach_retries_through_residual_conflict(self):
        """A residual ConflictException on the attach retries within the budget."""
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}
        client.update_agent.side_effect = [ConflictException('busy'), None]

        with patch('cid.helpers.quicksight.agent.time.sleep'):
            helper.create_or_update(definition, SPACE_ARNS)

        assert client.update_agent.call_count == 2
        assert client.update_agent.call_args.kwargs['SpacesToAdd'] == sorted(SPACE_ARNS)


class TestWriteProvenanceMutationSafety:
    """write_provenance never mutates a transitional (CREATING/UPDATING) agent: it
    waits for the agent to settle before any UpdateAgent/TagResource call."""

    AGENT_ARN = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:agent/cid-finops-advisor'

    def test_fresh_agent_with_baked_marker_settles_then_tags_without_update(self):
        """Marker baked at create, agent settles CREATING -> UPDATING -> ACTIVE,
        tag written after settle, UpdateAgent NEVER called."""
        helper, client = make_agent_helper()
        description = f'A test agent {CID_MANAGED_MARKER}'
        client.describe_agent.side_effect = [
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': description, 'AgentStatus': 'CREATING'}},
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': description, 'AgentStatus': 'UPDATING'}},
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': description, 'AgentStatus': 'ACTIVE'}},
        ]
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            helper.write_provenance(self.AGENT_ARN, 'cid-finops-advisor')
        client.update_agent.assert_not_called()   # no mutation of a transitional agent
        client.tag_resource.assert_called_once()  # tag only after ACTIVE
        assert mock_sleep.call_count == 2

    def test_transitional_agent_is_never_mutated_marker_written_after_settle(self):
        """A marker-less agent that is still UPDATING is not touched until it
        settles; the marker UpdateAgent fires only against the ACTIVE agent."""
        helper, client = make_agent_helper()
        client.describe_agent.side_effect = [
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': 'A test agent', 'AgentStatus': 'UPDATING'}},
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': 'A test agent', 'AgentStatus': 'ACTIVE'}},
            # post-marker-write settle check before the tag
            {'Agent': {'Name': 'CID FinOps Advisor', 'Description': f'A test agent {CID_MANAGED_MARKER}', 'AgentStatus': 'ACTIVE'}},
        ]
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            helper.write_provenance(self.AGENT_ARN, 'cid-finops-advisor')
        client.update_agent.assert_called_once()
        payload = client.update_agent.call_args.kwargs
        assert payload['Name'] == 'CID FinOps Advisor'
        assert CID_MANAGED_MARKER in payload['Description']
        assert payload['Description'].startswith('A test agent')
        client.tag_resource.assert_called_once()
        assert mock_sleep.call_count == 1

    def test_residual_conflict_on_settled_agent_retries_until_written(self):
        """A residual ConflictException flap on an ACTIVE agent still retries
        ( pattern) until the marker write succeeds."""
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {
            'Agent': {'Name': 'CID FinOps Advisor', 'Description': 'A test agent', 'AgentStatus': 'ACTIVE'},
        }
        client.update_agent.side_effect = [
            ConflictException('Cannot update Agent cid-finops-advisor in UPDATING status'),
            ConflictException('Cannot update Agent cid-finops-advisor in UPDATING status'),
            None,
        ]
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            helper.write_provenance(self.AGENT_ARN, 'cid-finops-advisor')  # must not raise
        assert client.update_agent.call_count == 3
        assert mock_sleep.call_count == 2
        payload = client.update_agent.call_args.kwargs
        assert payload['Name'] == 'CID FinOps Advisor'
        assert CID_MANAGED_MARKER in payload['Description']

    def test_never_settles_skips_marker_write_without_mutating(self, caplog):
        """An agent that never leaves UPDATING is NEVER mutated: the marker write
        is skipped with a WARNING after the budget, and the tag is still attempted
        (is_cid_managed accepts either mechanism)."""
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {
            'Agent': {'Name': 'CID FinOps Advisor', 'Description': '', 'AgentStatus': 'UPDATING'},
        }
        with patch('cid.helpers.quicksight.agent.time.sleep') as mock_sleep:
            with caplog.at_level(logging.WARNING, logger='cid.helpers.quicksight.agent'):
                helper.write_provenance(  # must NOT raise
                    self.AGENT_ARN, 'cid-finops-advisor', timeout=300, interval=5)
        # Key assertion: zero mutating calls against a transitional agent.
        client.update_agent.assert_not_called()
        # 300s bound at 5s intervals, then warn-and-skip (never a crash).
        assert mock_sleep.call_count == 300 // 5
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            'cid-finops-advisor' in r.getMessage() and 'provenance' in r.getMessage().lower()
            for r in warnings
        )
        # The provenance tag (mechanism (b)) was still attempted.
        client.tag_resource.assert_called_once()


class TestUpdateAgentFullReplacementSemantics:
    """Every UpdateAgent call must carry the deployed configuration through and
    must never send the create-only AgentLifecycle parameter."""

    AGENT_ARN = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:agent/cid-finops-advisor'

    def _deployed(self):
        definition = base_definition()
        return definition, deployed_agent_matching(definition, SPACE_ARNS)

    def test_marker_write_carries_deployed_configuration_through(self):
        """The Description-marker write must include the deployed StarterPrompts,
        WelcomeMessage and CustomPromptInput so the full-replacement UpdateAgent
        does not clear them."""
        definition, deployed = self._deployed()
        deployed['Description'] = 'A test agent'  # no marker yet
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}

        helper.write_provenance(self.AGENT_ARN, definition['agentId'])

        payload = client.update_agent.call_args.kwargs
        assert CID_MANAGED_MARKER in payload['Description']
        assert payload['StarterPrompts'] == list(definition['starterPrompts'])
        assert payload['WelcomeMessage'] == definition['welcomeMessage']
        new_prompt = payload['CustomPromptInput']['NewPrompt']
        assert set(new_prompt) == set(PERSONA_API_FIELDS)  # server-derived keys dropped

    def test_tag_written_after_marker_write(self):
        """The provenance tag is written AFTER the marker write settles: TagResource
        issued while a fresh agent is still CREATING is accepted (202) but is not
        durably applied until the agent settles."""
        definition, deployed = self._deployed()
        deployed['Description'] = 'A test agent'
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}
        order = []
        client.update_agent.side_effect = lambda **kw: order.append('update_agent')
        client.tag_resource.side_effect = lambda **kw: order.append('tag_resource')

        helper.write_provenance(self.AGENT_ARN, definition['agentId'])

        assert order == ['update_agent', 'tag_resource']

    def test_update_never_sends_agent_lifecycle(self):
        """UpdateAgent does not accept AgentLifecycle (raises ParamValidationError)."""
        definition, deployed = self._deployed()
        deployed['WelcomeMessage'] = 'drifted'
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.update_agent.call_args.kwargs
        assert 'AgentLifecycle' not in payload

    def test_lifecycle_drift_alone_is_noop_with_warning(self, caplog):
        """Lifecycle is create-only: drift on it alone makes no API call and warns."""
        definition, deployed = self._deployed()
        deployed['AgentLifecycle'] = 'DRAFT'  # catalog wants PUBLISHED
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}

        with caplog.at_level(logging.WARNING, logger='cid.helpers.quicksight.agent'):
            result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'unchanged'
        client.update_agent.assert_not_called()
        assert any('lifecycle' in r.getMessage().lower() for r in caplog.records)

    def test_update_carries_deployed_description_through(self):
        """A drift update must carry the deployed Description (which holds the CID
        provenance marker) through, or the full-replacement UpdateAgent clears it."""
        definition, deployed = self._deployed()
        deployed['Description'] = f'A test agent {CID_MANAGED_MARKER}'
        deployed['WelcomeMessage'] = 'drifted'
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}

        helper.create_or_update(definition, SPACE_ARNS)

        payload = client.update_agent.call_args.kwargs
        assert payload['Description'] == f'A test agent {CID_MANAGED_MARKER}'

    def test_update_carries_unmanaged_fields_through(self):
        """Fields the definition does not manage (None) must still be sent with the
        deployed values so the full-replacement UpdateAgent does not clear them."""
        definition, deployed = self._deployed()
        managed_only_name = {
            'agentId': definition['agentId'],
            'name': definition['name'] + ' Renamed',   # name drift triggers the update
            'persona': None,
            'starterPrompts': None,
            'welcomeMessage': None,
            'lifecycle': None,
        }
        helper, client = make_agent_helper()
        client.describe_agent.return_value = {'Agent': deployed}

        helper.create_or_update(managed_only_name, SPACE_ARNS)

        payload = client.update_agent.call_args.kwargs
        assert payload['Name'] == definition['name'] + ' Renamed'
        assert payload['StarterPrompts'] == list(definition['starterPrompts'])
        assert payload['WelcomeMessage'] == definition['welcomeMessage']
        assert set(payload['CustomPromptInput']['NewPrompt']) == set(PERSONA_API_FIELDS)


# ======================================================================
# from test_space_helper.py
# ======================================================================


def make_space(resources=None):
    """Build a Space over a fully mocked boto3 session/client.

    ``client.exceptions.*`` must be real exception classes so ``except`` clauses work.
    """
    client = MagicMock()
    client.exceptions.ResourceNotFoundException = type('ResourceNotFoundException', (Exception,), {})
    client.exceptions.ResourceExistsException = type('ResourceExistsException', (Exception,), {})
    client.exceptions.AccessDeniedException = type('AccessDeniedException', (Exception,), {})
    client.exceptions.ClientError = type('ClientError', (Exception,), {})
    session = MagicMock()
    session.client.return_value = client
    session.region_name = 'us-east-1'
    session.get_partition_for_region.return_value = 'aws'
    space = Space(session, resources=resources)
    space.awsIdentity = {'Account': ACCOUNT_ID}  # avoid the STS call in CidBase
    return space, client, session


def test_space_subclasses_cidbase():
    assert issubclass(Space, CidBase)


def test_constructor_obtains_quicksight_client_from_session():
    space, client, session = make_space()
    session.client.assert_called_once_with('quicksight')
    assert space.client is client


def test_constructor_stores_resources():
    resources = {'agents': {'finops': {}}}
    space, _, _ = make_space(resources=resources)
    assert space.resources == resources
    default_space, _, _ = make_space()
    assert default_space.resources == {}


def test_create_or_update_creates_when_space_does_not_exist():
    space, client, _ = make_space()
    client.describe_space.side_effect = client.exceptions.ResourceNotFoundException()
    arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.create_space.return_value = {'spaceArn': arn}

    result = space.create_or_update('my-space', 'My Space', description='desc')

    assert result == arn
    client.create_space.assert_called_once_with(
        AwsAccountId=ACCOUNT_ID, SpaceId='my-space', Name='My Space', Description='desc',
    )
    client.update_space.assert_not_called()


def test_create_or_update_updates_when_space_exists():
    space, client, _ = make_space()
    client.describe_space.return_value = {'Space': {'name': 'Old Name'}}
    arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.update_space.return_value = {'spaceArn': arn}

    result = space.create_or_update('my-space', 'New Name')

    assert result == arn
    client.update_space.assert_called_once_with(
        AwsAccountId=ACCOUNT_ID, SpaceId='my-space', Name='New Name',
    )
    client.create_space.assert_not_called()


def test_create_or_update_falls_back_to_update_on_create_race():
    space, client, _ = make_space()
    client.describe_space.side_effect = client.exceptions.ResourceNotFoundException()
    client.create_space.side_effect = client.exceptions.ResourceExistsException()
    arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.update_space.return_value = {'spaceArn': arn}

    result = space.create_or_update('my-space', 'My Space')

    assert result == arn
    client.create_space.assert_called_once()
    client.update_space.assert_called_once()


def _arn(suffix):
    return f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/{suffix}'


def test_update_resources_adds_only_missing_arns():
    space, client, _ = make_space()
    existing = _arn('already-there')
    new = _arn('new-dashboard')
    client.list_space_resources.return_value = {
        'Resources': [{'ResourceType': 'DASHBOARD', 'ResourceDetails': {'resourceArn': existing}}],
    }
    client.update_space_resources.return_value = {'FailedResourceOperations': []}

    failed = space.update_resources('my-space', [existing, new])

    assert failed == []
    client.update_space_resources.assert_called_once()
    kwargs = client.update_space_resources.call_args.kwargs
    added_arns = [item['ResourceDetails']['resourceArn'] for item in kwargs['AddResources']]
    assert added_arns == [new]  # only the ARN not already present
    assert 'RemoveResources' not in kwargs  # strictly additive by default


def test_update_resources_noop_when_everything_present():
    space, client, _ = make_space()
    existing = _arn('already-there')
    client.list_space_resources.return_value = {
        'Resources': [{'ResourceType': 'DASHBOARD', 'ResourceDetails': {'resourceArn': existing}}],
    }

    failed = space.update_resources('my-space', [existing])

    assert failed == []
    client.update_space_resources.assert_not_called()


def test_update_resources_reports_partial_failures_and_continues(caplog):
    space, client, _ = make_space()
    bad = _arn('denied-dashboard')
    good = _arn('fine-dashboard')
    client.list_space_resources.return_value = {'Resources': []}
    failure = {
        'ResourceType': 'DASHBOARD',
        'ResourceDetails': {'resourceArn': bad},
        'ErrorMessage': 'Access denied',
    }
    client.update_space_resources.return_value = {'FailedResourceOperations': [failure]}

    with caplog.at_level(logging.WARNING, logger='cid.helpers.quicksight.space'):
        failed = space.update_resources('my-space', [good, bad])

    assert failed == [failure]  # the failed list is returned to the caller
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(bad in r.getMessage() and 'Access denied' in r.getMessage() for r in warnings)


def test_grant_owner_sends_exactly_the_16_action_owner_set():
    space, client, _ = make_space()
    principal = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/admin'

    space.grant_owner('my-space', principal)

    client.update_space_permissions.assert_called_once()
    kwargs = client.update_space_permissions.call_args.kwargs
    assert kwargs['AwsAccountId'] == ACCOUNT_ID
    assert kwargs['SpaceId'] == 'my-space'
    grants = kwargs['GrantPermissions']
    assert len(grants) == 1
    assert grants[0]['Principal'] == principal
    assert set(grants[0]['Actions']) == set(SPACE_OWNER_ACTIONS)
    assert len(grants[0]['Actions']) == 16


def test_find_by_name_single_match():
    space, client, _ = make_space()
    client.search_spaces.return_value = {'SpaceSummaries': [{'spaceId': 'space-1'}]}

    assert space.find_by_name('My Space') == ['space-1']
    kwargs = client.search_spaces.call_args.kwargs
    assert kwargs['Filters'] == [{'name': 'SPACE_NAME', 'operator': 'STRING_EQUALS', 'value': 'My Space'}]


def test_find_by_name_multiple_matches():
    space, client, _ = make_space()
    client.search_spaces.return_value = {
        'SpaceSummaries': [{'spaceId': 'space-1'}, {'spaceId': 'space-2'}],
    }

    assert space.find_by_name('My Space') == ['space-1', 'space-2']


def test_find_by_name_zero_matches():
    space, client, _ = make_space()
    client.search_spaces.return_value = {'SpaceSummaries': []}

    assert space.find_by_name('No Such Space') == []


def test_write_provenance_tags_and_writes_description_marker():
    space, client, _ = make_space()
    space_arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.describe_space.return_value = {
        'Space': {'name': 'My Space', 'description': 'FinOps knowledge'},
    }

    space.write_provenance(space_arn, 'my-space')

    client.tag_resource.assert_called_once_with(
        ResourceArn=space_arn,
        Tags=[{'Key': CID_PROVENANCE_TAG_KEY, 'Value': CID_PROVENANCE_TAG_VALUE}],
    )
    client.update_space.assert_called_once()
    kwargs = client.update_space.call_args.kwargs
    assert kwargs['SpaceId'] == 'my-space'
    assert kwargs['Name'] == 'My Space'
    assert CID_MANAGED_MARKER in kwargs['Description']
    assert kwargs['Description'].startswith('FinOps knowledge')  # original description preserved


def test_write_provenance_tolerates_tag_failure_and_still_writes_marker():
    space, client, _ = make_space()
    space_arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.tag_resource.side_effect = client.exceptions.AccessDeniedException()
    client.describe_space.return_value = {'Space': {'name': 'My Space', 'description': ''}}

    space.write_provenance(space_arn, 'my-space')  # must not raise

    client.update_space.assert_called_once()
    kwargs = client.update_space.call_args.kwargs
    assert CID_MANAGED_MARKER in kwargs['Description']


def test_write_provenance_skips_update_when_marker_already_present():
    space, client, _ = make_space()
    space_arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.describe_space.return_value = {
        'Space': {'name': 'My Space', 'description': f'Something. {CID_MANAGED_MARKER}'},
    }

    space.write_provenance(space_arn, 'my-space')

    client.update_space.assert_not_called()


_space_ids = st.text(
    alphabet='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_=.+',
    min_size=1, max_size=40,
)


_principal_arns = st.text(min_size=1, max_size=60).map(
    lambda s: f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/{s}'
)


@settings(max_examples=100, deadline=None)
@given(space_id=_space_ids, principal_arn=_principal_arns)
def test_property_17_space_owner_grant_uses_exactly_the_canonical_action_set(space_id, principal_arn):
    space, client, _ = make_space()

    space.grant_owner(space_id, principal_arn)

    client.update_space_permissions.assert_called_once()
    kwargs = client.update_space_permissions.call_args.kwargs
    grants = kwargs['GrantPermissions']
    assert len(grants) == 1
    assert grants[0]['Principal'] == principal_arn
    actions = grants[0]['Actions']
    # exactly the 16 canonical owner actions: no more, no fewer, no duplicates
    assert len(actions) == 16
    assert set(actions) == set(SPACE_OWNER_ACTIONS)
    assert kwargs['SpaceId'] == space_id


def test_write_provenance_conflict_settles_then_marker_written():
    """UpdateSpace ConflictException while the Space settles is retried until
    the Description marker write succeeds (must not raise)."""
    from unittest.mock import patch

    space, client, _ = make_space()
    client.exceptions.ConflictException = type('ConflictException', (Exception,), {})
    space_arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/my-space'
    client.describe_space.return_value = {
        'Space': {'name': 'My Space', 'description': 'FinOps knowledge'},
    }
    client.update_space.side_effect = [
        client.exceptions.ConflictException('Cannot update Space my-space in UPDATING status'),
        client.exceptions.ConflictException('Cannot update Space my-space in UPDATING status'),
        None,
    ]

    with patch('cid.helpers.quicksight.space.time.sleep') as mock_sleep:
        space.write_provenance(space_arn, 'my-space')  # must not raise

    assert client.update_space.call_count == 3
    assert mock_sleep.call_count == 2
    kwargs = client.update_space.call_args.kwargs
    assert kwargs['Name'] == 'My Space'
    assert CID_MANAGED_MARKER in kwargs['Description']
    assert kwargs['Description'].startswith('FinOps knowledge')


# ======================================================================
# UpdateAgent partial association failures + Space link repair
# ======================================================================


class TestAssociationFailureChecks:
    """UpdateAgent reports partial association failures IN-BAND (FailedToAddSpaces /
    FailedToRemoveSpaces and the action-connector twins), not as exceptions: an HTTP
    200 can carry per-ARN failures. Every association-carrying UpdateAgent checks the
    response and raises, or the deployed associations silently diverge."""

    def test_attach_raises_when_the_response_reports_a_failed_add(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}
        client.update_agent.return_value = {
            'FailedToAddSpaces': [
                {'Arn': SPACE_ARNS[0], 'ErrorCode': 'AccessDenied', 'ErrorMessage': 'not authorized'},
            ],
        }

        with pytest.raises(CidError, match='association failures') as excinfo:
            helper.create_or_update(definition, SPACE_ARNS)

        assert SPACE_ARNS[0] in str(excinfo.value)
        assert 'AccessDenied' in str(excinfo.value)

    def test_update_raises_when_the_response_reports_a_failed_remove(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        stale_arn = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/stale-space'
        deployed = deployed_agent_matching(definition, SPACE_ARNS + [stale_arn])
        client.describe_agent.return_value = {'Agent': deployed}
        client.update_agent.return_value = {
            'FailedToRemoveSpaces': [{'Arn': stale_arn, 'ErrorCode': 'InternalFailure'}],
        }

        with pytest.raises(CidError, match='association failures') as excinfo:
            helper.create_or_update(definition, SPACE_ARNS, sync_spaces=True)

        assert stale_arn in str(excinfo.value)

    def test_a_clean_response_raises_nothing(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        wire_create_lifecycle(client, created_agent_shape(definition))
        client.create_agent.return_value = {'Arn': 'arn:new-agent'}
        client.update_agent.return_value = {'Arn': 'arn:new-agent', 'AgentStatus': 'UPDATING'}

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'created'


class TestRepairSpaceAssociations:
    """repair_space_associations rewrites the Space links: one UpdateAgent detaching
    every attached Space, a settle wait, then one UpdateAgent re-attaching the same
    ARNs. Two calls because the API rejects the same ARN in both lists of one call."""

    @staticmethod
    def wire_repair_lifecycle(client, definition, space_arns):
        """describe_agent returns the deployed agent; its Spaces empty once the
        detach UpdateAgent has fired (so the re-attach sees them as missing)."""
        deployed = deployed_agent_matching(definition, space_arns)

        def describe(**kwargs):
            spaces = [] if client.update_agent.call_count else sorted(space_arns)
            return {'Agent': dict(deployed, Spaces=spaces)}
        client.describe_agent.side_effect = describe

    def test_repair_detaches_then_reattaches_the_same_spaces(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        self.wire_repair_lifecycle(client, definition, SPACE_ARNS)
        client.update_agent.return_value = {}

        assert helper.repair_space_associations(definition['agentId']) is True

        assert client.update_agent.call_count == 2
        detach, reattach = (call.kwargs for call in client.update_agent.call_args_list)
        assert detach['SpacesToRemove'] == sorted(SPACE_ARNS)
        assert 'SpacesToAdd' not in detach
        assert reattach['SpacesToAdd'] == sorted(SPACE_ARNS)
        assert 'SpacesToRemove' not in reattach

    def test_repair_carries_the_deployed_config_through_both_calls(self):
        """Full-replacement semantics: both UpdateAgent calls carry the deployed
        Name/Description/StarterPrompts/WelcomeMessage/persona through."""
        helper, client = make_agent_helper()
        definition = base_definition()
        self.wire_repair_lifecycle(client, definition, SPACE_ARNS)
        client.update_agent.return_value = {}

        helper.repair_space_associations(definition['agentId'])

        for call in client.update_agent.call_args_list:
            assert call.kwargs['Name'] == definition['name']
            assert call.kwargs['StarterPrompts'] == definition['starterPrompts']
            assert call.kwargs['WelcomeMessage'] == definition['welcomeMessage']
            assert 'AgentLifecycle' not in call.kwargs

    def test_repair_without_spaces_is_a_noop(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        client.describe_agent.return_value = {'Agent': deployed_agent_matching(definition, [])}

        assert helper.repair_space_associations(definition['agentId']) is False

        client.update_agent.assert_not_called()

    def test_repair_raises_on_a_missing_agent(self):
        helper, client = make_agent_helper()
        client.describe_agent.side_effect = ResourceNotFoundException('not found')

        with pytest.raises(CidError, match='not found'):
            helper.repair_space_associations('missing-agent')

    def test_failed_reattach_names_the_detached_spaces_and_the_recovery(self):
        """When the re-attach fails after a successful detach, the agent is left
        without its Spaces; the error says which and how to recover."""
        helper, client = make_agent_helper()
        definition = base_definition()
        self.wire_repair_lifecycle(client, definition, SPACE_ARNS)
        client.update_agent.side_effect = [{}, RuntimeError('boom')]

        with pytest.raises(CidError, match='Re-run this command') as excinfo:
            helper.repair_space_associations(definition['agentId'])

        assert SPACE_ARNS[0] in str(excinfo.value)

    def test_repair_detach_retries_through_residual_conflict(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        self.wire_repair_lifecycle(client, definition, SPACE_ARNS)
        client.update_agent.side_effect = [ConflictException('busy'), {}, {}]

        with patch('cid.helpers.quicksight.agent.time.sleep'):
            assert helper.repair_space_associations(definition['agentId']) is True

        assert client.update_agent.call_count == 3


class TestAdditiveSpaceLinks:
    """Space associations are ADDITIVE by default on update: Spaces the user
    attached outside the catalog are never detached. sync_spaces=True opts into
    exact synchronization."""

    @staticmethod
    def deployed_with_extra_space(definition):
        extra = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:space/user-added-space'
        return deployed_agent_matching(definition, SPACE_ARNS + [extra]), extra

    def test_default_never_detaches_a_user_added_space(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        deployed, _ = self.deployed_with_extra_space(definition)
        client.describe_agent.return_value = {'Agent': deployed}

        result = helper.create_or_update(definition, SPACE_ARNS)

        assert result['action'] == 'unchanged'
        client.update_agent.assert_not_called()

    def test_sync_spaces_detaches_spaces_the_catalog_does_not_carry(self):
        helper, client = make_agent_helper()
        definition = base_definition()
        deployed, extra = self.deployed_with_extra_space(definition)
        client.describe_agent.return_value = {'Agent': deployed}
        client.update_agent.return_value = {}

        result = helper.create_or_update(definition, SPACE_ARNS, sync_spaces=True)

        assert result['action'] == 'updated'
        assert client.update_agent.call_args.kwargs['SpacesToRemove'] == [extra]
