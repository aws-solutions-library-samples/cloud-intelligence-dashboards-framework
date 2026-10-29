""" Quick chat Agent helper: describe / create / update / delete / permissions.

Agents are served by the ``quicksight`` boto3 client. UpdateAgent has FULL-REPLACEMENT
semantics for the configuration fields (omitted fields are cleared), so every update
starts from the deployed configuration and overrides only what it means to change.
"""
import logging

from cid.base import CidBase
from cid.exceptions import CidError
from cid.helpers.quicksight.agent_logic import (
    CID_MANAGED_MARKER,
    PERSONA_API_FIELDS,
    association_failures,
    compute_space_delta,
    persona_differs,
    poll,
    retry_on_conflict,
)

logger = logging.getLogger(__name__)

# Agent permissions must be granted as this complete set in one call
# (piecemeal single-action grants fail with InvalidParameterValueException).
AGENT_OWNER_ACTIONS = [
    'quicksight:DescribeAgent',
    'quicksight:UpdateAgent',
    'quicksight:DeleteAgent',
    'quicksight:DescribeAgentPermissions',
    'quicksight:UpdateAgentPermissions',
]
TRANSITIONAL_STATUSES = ('CREATING', 'UPDATING')


class Agent(CidBase):
    """ Lifecycle operations for Quick chat Agents """

    def __init__(self, session, resources=None) -> None:
        super().__init__(session)
        self.client = self.session.client('quicksight')
        self.resources = resources or {}

    def get(self, agent_id: str) -> dict:
        """ DescribeAgent; None when the agent does not exist (ListAgents omits some states) """
        try:
            return self.client.describe_agent(AwsAccountId=self.account_id, AgentId=agent_id).get('Agent')
        except self.client.exceptions.ResourceNotFoundException:
            logger.debug(f'Agent {agent_id!r} not found.')
            return None

    def grant_owner(self, agent_id: str, principal_arn: str) -> None:
        """ Grant the owner action set to a principal (one call) """
        logger.info(f'Granting agent owner permissions on {agent_id!r} to {principal_arn}')
        self.client.update_agent_permissions(
            AwsAccountId=self.account_id,
            AgentId=agent_id,
            GrantPermissions=[{'Principal': principal_arn, 'Actions': AGENT_OWNER_ACTIONS}],
        )

    def wait_active(self, agent_id: str, timeout: int=300, interval: int=5) -> dict:
        """ Wait for AgentStatus ACTIVE; raise immediately on FAILED and on timeout """
        agent, finished = poll(
            lambda: self.get(agent_id),
            lambda agent: (agent or {}).get('AgentStatus') in ('ACTIVE', 'FAILED'),
            timeout=timeout, interval=interval,
        )
        status = (agent or {}).get('AgentStatus')
        if status == 'FAILED':
            raise CidError(f'Agent {agent_id!r} entered FAILED status while waiting for ACTIVE. '
                           f"ErrorMessage: {agent.get('ErrorMessage')}")
        if not finished:
            raise CidError(f'Agent {agent_id!r} did not reach ACTIVE status within {timeout} seconds. '
                           f'Last observed status: {status}')
        logger.info(f'Agent {agent_id!r} is ACTIVE.')
        return agent

    def wait_settled(self, agent_id: str, timeout: int=300, interval: int=5) -> dict:
        """ Wait until the agent leaves CREATING/UPDATING; returns the last seen agent (never raises) """
        agent, finished = poll(
            lambda: self.get(agent_id),
            lambda agent: (agent or {}).get('AgentStatus') not in TRANSITIONAL_STATUSES,
            timeout=timeout, interval=interval,
        )
        if not finished:
            logger.warning(f"Agent {agent_id!r} did not settle out of {agent.get('AgentStatus')!r} within {timeout} seconds.")
        return agent

    def delete(self, agent_id: str, timeout: int=300, interval: int=5) -> None:
        """ Delete an agent, retrying while it is busy; a missing agent is treated as success """
        def _delete():
            try:
                self.client.delete_agent(AwsAccountId=self.account_id, AgentId=agent_id)
                logger.info(f'Deleted agent {agent_id!r}.')
            except self.client.exceptions.ResourceNotFoundException:
                logger.info(f'Agent {agent_id!r} does not exist. Nothing to delete.')
        try:
            retry_on_conflict(_delete, self.client.exceptions.ConflictException, timeout=timeout, interval=interval)
        except self.client.exceptions.ConflictException as exc:
            raise CidError(f'Could not delete agent {agent_id!r}: the agent was still busy '
                           f'(ConflictException) after {timeout} seconds. Last error: {exc}') from exc

    def _update_settled(self, agent_id: str, change, timeout: int=300, interval: int=5):
        """ UpdateAgent on a settled agent, carrying the deployed configuration through.

        ``change(params, agent)`` adjusts the carried-through params; it is re-applied
        on a freshly settled agent after each ConflictException. Returns the response.
        """
        def _update():
            agent = self.wait_settled(agent_id, timeout=timeout, interval=interval)
            if agent is None:
                raise CidError(f'Agent {agent_id!r} not found.')
            params = self._carry_through_update_params(agent, agent_id)
            change(params, agent)
            return self.client.update_agent(**params)
        return retry_on_conflict(_update, self.client.exceptions.ConflictException, timeout=timeout, interval=interval)

    def _carry_through_update_params(self, agent: dict, agent_id: str) -> dict:
        """ UpdateAgent params pre-filled with the deployed configuration (full-replacement semantics) """
        params = {'AwsAccountId': self.account_id, 'AgentId': agent_id, 'Name': agent.get('Name')}
        for field in ('Description', 'WelcomeMessage'):
            if agent.get(field):
                params[field] = agent[field]
        if agent.get('StarterPrompts'):
            params['StarterPrompts'] = list(agent['StarterPrompts'])
        deployed_prompt = self._persona_to_new_prompt(agent.get('CustomPromptInterface') or {})
        if deployed_prompt:
            params['CustomPromptInput'] = {'NewPrompt': deployed_prompt}
        return params

    @staticmethod
    def _raise_on_association_failures(response, agent_id: str) -> None:
        """ UpdateAgent reports Space/connector failures in-band (HTTP 200); raise on any """
        failures = association_failures(response)
        if failures:
            raise CidError(f'UpdateAgent on agent {agent_id!r} reported association failures: ' + '; '.join(failures))

    def write_marker(self, agent_id: str, timeout: int=300, interval: int=5) -> None:
        """ Ensure the CID-managed marker is in the agent description; leaves the agent settled """
        agent = self.wait_settled(agent_id, timeout=timeout, interval=interval)
        if agent is None or CID_MANAGED_MARKER in str(agent.get('Description') or ''):
            return
        if agent.get('AgentStatus') in TRANSITIONAL_STATUSES:  # never mutate an agent that did not settle
            logger.warning(f'Skipped the CID-managed marker on agent {agent_id!r}: still {agent["AgentStatus"]} '
                           f'after {timeout}s. The provenance tag is still written.')
            return

        def _add_marker(params, agent):
            params['Description'] = f"{agent.get('Description') or ''} {CID_MANAGED_MARKER}".strip()
        try:
            self._update_settled(agent_id, _add_marker, timeout=timeout, interval=interval)
            self.wait_settled(agent_id, timeout=timeout, interval=interval)  # the update republishes
        except self.client.exceptions.ConflictException as exc:
            # the provenance tag is written separately, so CID-managed detection still works
            logger.warning(f'Could not write the CID-managed marker on agent {agent_id!r}: still busy after {timeout}s ({exc}).')

    @staticmethod
    def _persona_to_new_prompt(persona: dict) -> dict:
        """ Map a persona (catalog camelCase or API PascalCase keys) to the NewPrompt write shape """
        lowered = {str(key).lower(): value for key, value in (persona or {}).items() if value is not None}
        return {field: str(lowered[field.lower()]) for field in PERSONA_API_FIELDS if field.lower() in lowered}

    def create_or_update(self, definition: dict, space_arns, sync_spaces: bool=False,
                         sync_connectors: bool=False) -> dict:
        """ Create the agent, update what drifted, or do nothing.

        Spaces and action connectors are additive unless ``sync_spaces`` /
        ``sync_connectors`` ask for an exact match. ``lifecycle`` is create-only.
        Returns ``{'agentId', 'arn', 'action'}`` with action created/updated/unchanged.
        """
        agent_id = definition.get('agentId')
        desired_spaces = set(space_arns or ())
        desired_connectors = set((definition.get('dependsOn') or {}).get('actionConnectors') or ())
        agent = self.get(agent_id)
        if agent is None:
            return self._create(definition, desired_spaces, desired_connectors)
        return self._update(agent, definition, desired_spaces, desired_connectors, sync_spaces, sync_connectors)

    def _create(self, definition: dict, space_arns: set, connector_arns: set) -> dict:
        """ CreateAgent with the marker in the description; Spaces are attached once it settles """
        agent_id = definition.get('agentId')
        description = str(definition.get('description') or '')
        params = {
            'AwsAccountId': self.account_id,
            'AgentId': agent_id,
            'Name': definition.get('name'),
            'Description': description if CID_MANAGED_MARKER in description else f'{description} {CID_MANAGED_MARKER}'.strip(),
        }
        new_prompt = self._persona_to_new_prompt(definition.get('persona') or {})
        if new_prompt:
            params['CustomPromptInput'] = {'NewPrompt': new_prompt}
        if definition.get('starterPrompts') is not None:
            params['StarterPrompts'] = list(definition['starterPrompts'])
        if definition.get('welcomeMessage') is not None:
            params['WelcomeMessage'] = definition['welcomeMessage']
        if definition.get('lifecycle') is not None:
            params['AgentLifecycle'] = definition['lifecycle']
        if connector_arns:
            params['ActionConnectors'] = sorted(connector_arns)
        logger.info(f'Creating agent {agent_id!r}.')
        try:
            arn = self.client.create_agent(**params).get('Arn')
        except self.client.exceptions.ResourceExistsException:
            logger.info(f'Agent {agent_id!r} already exists (ResourceExistsException). Treating as success.')
            arn = (self.get(agent_id) or {}).get('Arn')
        if space_arns:
            self._attach_spaces(agent_id, space_arns)
        return {'agentId': agent_id, 'arn': arn, 'action': 'created'}

    def _attach_spaces(self, agent_id: str, space_arns, timeout: int=300, interval: int=5) -> None:
        """ Attach Spaces the agent does not carry yet, via UpdateAgent SpacesToAdd on a settled agent """
        def _add(params, agent):
            params['SpacesToAdd'] = sorted(set(space_arns) - set(agent.get('Spaces') or ()))
        agent = self.wait_settled(agent_id, timeout=timeout, interval=interval)
        if agent is None:
            raise CidError(f'Cannot attach Spaces to agent {agent_id!r}: the agent was not found after create.')
        if not set(space_arns) - set(agent.get('Spaces') or ()):
            return
        logger.info(f'Attaching Space(s) to agent {agent_id!r}.')
        try:
            response = self._update_settled(agent_id, _add, timeout=timeout, interval=interval)
        except self.client.exceptions.ConflictException as exc:
            raise CidError(f'Could not attach Spaces to agent {agent_id!r}: the agent was still busy '
                           f'(ConflictException) after {timeout} seconds. Last error: {exc}') from exc
        self._raise_on_association_failures(response, agent_id)

    def repair_space_associations(self, agent_id: str, timeout: int=300, interval: int=5) -> bool:
        """ Rewrite the agent's Space links: detach every Space, then re-attach it.

        Recovers an agent whose Space shows as unavailable although it describes as
        healthy. Two calls, because the API rejects one ARN in both lists of a call.
        Returns False when the agent has no Space.
        """
        agent = self.wait_settled(agent_id, timeout=timeout, interval=interval)
        if agent is None:
            raise CidError(f'Cannot repair agent {agent_id!r}: the agent was not found.')
        space_arns = sorted(set(agent.get('Spaces') or ()))
        if not space_arns:
            logger.info(f'Agent {agent_id!r} has no Space to repair.')
            return False
        logger.info(f'Repairing agent {agent_id!r}: detaching and re-attaching {len(space_arns)} Space(s).')

        def _detach(params, agent):
            params['SpacesToRemove'] = space_arns
        try:
            response = self._update_settled(agent_id, _detach, timeout=timeout, interval=interval)
        except self.client.exceptions.ConflictException as exc:
            raise CidError(f'Could not detach Spaces from agent {agent_id!r}: the agent was still busy '
                           f'(ConflictException) after {timeout} seconds. Last error: {exc}') from exc
        self._raise_on_association_failures(response, agent_id)
        try:
            self._attach_spaces(agent_id, space_arns, timeout=timeout, interval=interval)
        except self.client.exceptions.AccessDeniedException:
            raise
        except Exception as exc:
            raise CidError(f'The Space re-attach failed while repairing agent {agent_id!r}, leaving it detached '
                           f'from: {", ".join(space_arns)}. Re-run this command to re-attach. Error: {exc}') from exc
        self.wait_settled(agent_id, timeout=timeout, interval=interval)
        return True

    def _update(self, agent: dict, definition: dict, desired_spaces: set, desired_connectors: set,
                sync_spaces: bool=False, sync_connectors: bool=False) -> dict:
        """ UpdateAgent for the fields that drifted, or no call when nothing did """
        agent_id = definition.get('agentId')
        name = definition.get('name')
        desired_persona = definition.get('persona') or {}
        starter_prompts = definition.get('starterPrompts')  # None = not managed by this definition
        welcome_message = definition.get('welcomeMessage')  # None = not managed by this definition
        lifecycle = definition.get('lifecycle')
        spaces_to_add, spaces_to_remove = compute_space_delta(desired_spaces, agent.get('Spaces'))
        connectors_to_add, connectors_to_remove = compute_space_delta(desired_connectors, agent.get('ActionConnectors'))
        if not sync_spaces:
            spaces_to_remove = set()  # additive: keep Spaces attached outside the catalog
        if not sync_connectors:
            connectors_to_remove = set()  # additive: keep connectors attached outside the catalog
        if lifecycle is not None and lifecycle != agent.get('AgentLifecycle'):
            logger.warning(f"Agent {agent_id!r} lifecycle is {agent.get('AgentLifecycle')!r} but the catalog wants "
                           f'{lifecycle!r}. The lifecycle can only be set at creation and is left unchanged.')
        drift = {
            'persona': bool(desired_persona) and persona_differs(desired_persona, agent.get('CustomPromptInterface') or {}),
            'spaces': bool(spaces_to_add or spaces_to_remove),
            'connectors': bool(connectors_to_add or connectors_to_remove),
            'starter_prompts': starter_prompts is not None and list(starter_prompts) != list(agent.get('StarterPrompts') or ()),
            'welcome_message': welcome_message is not None and str(welcome_message) != str(agent.get('WelcomeMessage') or ''),
            'name': name is not None and name != agent.get('Name'),
        }
        if not any(drift.values()):
            logger.info(f'Agent {agent_id!r} is up to date. No change needed.')
            return {'agentId': agent_id, 'arn': agent.get('Arn'), 'action': 'unchanged'}
        logger.info(f"Agent {agent_id!r} configuration drifted ({', '.join(sorted(k for k, v in drift.items() if v))}). Updating.")
        params = self._carry_through_update_params(agent, agent_id)
        if name is not None:
            params['Name'] = name
        new_prompt = self._persona_to_new_prompt(desired_persona)
        if new_prompt:
            params['CustomPromptInput'] = {'NewPrompt': new_prompt}
        if starter_prompts is not None:
            params['StarterPrompts'] = list(starter_prompts)
        if welcome_message is not None:
            params['WelcomeMessage'] = welcome_message
        for key, values in (('SpacesToAdd', spaces_to_add), ('SpacesToRemove', spaces_to_remove),
                            ('ActionConnectorsToAdd', connectors_to_add), ('ActionConnectorsToRemove', connectors_to_remove)):
            if values:
                params[key] = sorted(values)
        response = self.client.update_agent(**params)
        self._raise_on_association_failures(response, agent_id)
        return {'agentId': agent_id, 'arn': agent.get('Arn'), 'action': 'updated'}
