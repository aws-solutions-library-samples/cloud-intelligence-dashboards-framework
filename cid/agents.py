""" Quick Agents commands: create-agent / update-agent / list-agents / delete-agent.

Mixed into :class:`cid.common.Cid`; the ``@command`` entry points stay in common.py and
call the methods here. A Space owns the knowledge (dashboards, datasets, knowledge
bases); an agent references its Space and carries persona, prompts and action
connectors. These commands never deploy dashboards or touch the data layer.
"""
import time
import shutil
import logging
from functools import cached_property

import yaml

from cid.utils import get_parameter, get_parameters, set_parameters, get_yesno_parameter, cid_print, isatty
from cid.exceptions import CidError, CidCritical
from cid.helpers import Dataset
from cid.helpers.quicksight.space import Space
from cid.helpers.quicksight.agent import Agent
from cid.helpers.quicksight.agent_logic import (
    CID_PROVENANCE_TAG_KEY,
    CID_PROVENANCE_TAG_VALUE,
    build_agent_listing,
    build_console_url,
    classify_dependencies,
    compute_space_removals,
    derive_space_id,
    describe_drift,
    is_cid_managed,
    validate_caps,
)

logger = logging.getLogger(__name__)


class AgentCommands():
    """ Quick Agents commands, mixed into Cid """

    # Deployment guidance pointers. These messages MUST NOT name any cid-cmd command.
    CID_DASHBOARDS_GUIDANCE = ('the CID dashboards deployment guide '
                               '(https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html)')
    # QuickSight user roles entitled to create Quick Agents
    AGENT_AUTHOR_PRO_ROLES = ('AUTHOR_PRO', 'ADMIN_PRO')

    @cached_property
    def space(self) -> Space:
        return Space(self.base.session, resources=self.resources)

    @cached_property
    def agent(self) -> Agent:
        return Agent(self.base.session, resources=self.resources)

    def load_agentic_catalog(self):
        ''' load the agentic catalog (agent commands only; dashboard commands never need it).
        Skipped when agents were supplied via --catalog / --resources (e.g. a local checkout).
        '''
        if getattr(self, '_agentic_catalog_loaded', False):
            return
        self._agentic_catalog_loaded = True
        if (self.resources or {}).get('agents'):
            logger.debug('Agents already loaded from --catalog/--resources; not loading the default agentic catalog.')
            return
        for catalog_url in self.agentic_catalog_urls:
            self.load_catalog(catalog_url)

    def _list_agents(self):
        """List catalog agents with live status (DescribeAgent per entry), their Space and its dashboards."""
        self.load_agentic_catalog()
        agents_catalog = self.resources.get('agents') or {}
        if not agents_catalog:
            cid_print('No agents found in the catalog.')
            return {}
        deployed_ids = set()
        unknown_ids = set()
        for key, entry in agents_catalog.items():
            entry = entry if isinstance(entry, dict) else {}
            target_id = entry.get('agentId') or key
            try:
                if self.agent.get(target_id) is not None:  # live DescribeAgent
                    deployed_ids.add(target_id)
            except Exception as exc:
                logger.debug(f'DescribeAgent failed for {target_id!r} ({exc}). Marking unknown status.')
                unknown_ids.add(target_id)  # unknown status; continue with the rest
        # one read-only ListDashboards call covers all dependency annotations
        deployed_dashboard_arns = self._deployed_dashboard_arns_or_none()
        # ✓ marking, catalog order, each entry once; ? = status unknown
        listing = build_agent_listing(agents_catalog, deployed_ids)
        displays = [f" ?[{entry['agentId']}] {entry['name']}" if entry['agentId'] in unknown_ids else entry['display']
                    for entry in listing]
        for entry, label in zip(listing, self._agent_labels(listing, displays)):
            catalog_entry = agents_catalog.get(entry['key']) or {}
            spaces = (catalog_entry.get('dependsOn') or {}).get('spaces') or []
            suffix = f'  (Space {spaces[0]})' if spaces else ''
            if catalog_entry.get('providedBy'):
                suffix += f"  (provided by {catalog_entry['providedBy']})"
            cid_print(f'\n{label}')
            if suffix:
                cid_print(f'     {suffix.strip()}')
            if deployed_dashboard_arns is not None:
                for dependency_line in self._agent_dependency_lines(catalog_entry, deployed_dashboard_arns):
                    cid_print(dependency_line)
        return listing

    def _agent_dependency_lines(self, definition: dict, deployed_arns_by_id: dict,
                                color: bool=True, indent: str='     ', width: int=None) -> list:
        """Dependency summary lines for a listing entry: declared dashboards
        (agent + its Spaces) with a per-dashboard deployed indicator."""
        dashboards, _, _ = self._collect_agent_dependency_keys(definition)
        present_ids = self._present_dashboard_ids(dashboards, deployed_arns_by_id)
        return self._dependency_summary_lines(dashboards, present_ids, color=color, indent=indent, width=width)

    @staticmethod
    def _dependency_summary_lines(dashboards: list, present_keys,
                                  color: bool=True, indent: str='     ', width: int=None) -> list:
        """Format declared dashboard dependencies with per-dashboard deployed indicators.

        Returns a list of lines wrapped to the terminal width, continuation lines
        aligned under the first dashboard. ``color=False`` yields plain marks for
        surfaces that render literal text (the InquirerPy picker); the default carries
        cid_print color tags.
        """
        if not dashboards:
            return []
        if width is None:
            width = min(shutil.get_terminal_size((120, 24)).columns, 120)
        present = set(present_keys or ())
        prefix = indent + 'dashboards: '
        continuation = ' ' * len(prefix)
        lines, line_text, line_visible = [], prefix, len(prefix)
        for position, key in enumerate(dashboards):
            mark = '✓' if key in present else '✗'
            item_len = len(mark) + 1 + len(key)
            item_text = f'{mark} {key}' if not color else f"<{'GREEN' if key in present else 'YELLOW'}>{mark}<END> {key}"
            if position and line_visible + 2 + item_len > width:
                lines.append(line_text)
                line_text, line_visible = continuation + item_text, len(continuation) + item_len
            else:
                line_text += ('  ' if position else '') + item_text
                line_visible += (2 if position else 0) + item_len
        lines.append(line_text)
        return lines

    def _delete_agent(self, agent_id: str=None):
        """Delete a CID-managed agent after confirmation (never a dashboard); --delete-space also removes its Space when
        no other agent uses it, including the Space of an agent that is already deleted."""
        self.load_agentic_catalog()
        agents_catalog = self.resources.get('agents') or {}
        agent_key = agent_id or get_parameters().get('agent-id')
        if not agent_key:
            if not agents_catalog:
                raise CidError('No agents found in the catalog. Please provide --agent-id.')
            agent_key = self._select_agent('Please select an agent to delete')
        # tolerate either the catalog key or the deployed agentId as input
        definition = agents_catalog.get(agent_key)
        if definition is None:
            definition = next(
                (entry for entry in agents_catalog.values()
                 if isinstance(entry, dict) and entry.get('agentId') == agent_key),
                None,
            )
        target_agent_id = (definition or {}).get('agentId') or agent_key

        # missing target = success; existence via DescribeAgent, not ListAgents
        existing = self.agent.get(target_agent_id)
        if existing is None:
            if not get_parameters().get('delete-space'):
                cid_print(f'Agent <BOLD>{target_agent_id}<END> does not exist. Nothing to delete.')
                return target_agent_id
            # agent already gone: --delete-space still cleans up its catalog Space (same guards:
            # CID-managed, not used by another agent), confirmed because uploads are lost
            cid_print(f'Agent <BOLD>{target_agent_id}<END> does not exist. Checking its Space.')
            for space_id in sorted(self._agent_space_ids(definition, [])):
                if self.space.get(space_id) is None:
                    cid_print(f'Space <BOLD>{space_id}<END> does not exist. Nothing to delete.')
                    continue
                if not isatty() and not getattr(self, 'all_yes', False) \
                        and get_parameters().get('confirm-delete-space') is None:
                    cid_print(f'Space <BOLD>{space_id}<END> was kept: pass -y or --confirm-delete-space yes '
                              'to delete it without a terminal.')
                    continue
                if not get_yesno_parameter(
                        param_name='confirm-delete-space',
                        message=f'Delete Space {space_id!r}? (documents uploaded to it are lost; '
                                'no dashboard or dataset is deleted)',
                        default='no'):
                    cid_print(f'Space <BOLD>{space_id}<END> was kept.')
                    continue
                self._delete_agent_space(space_id, target_agent_id)
            return target_agent_id
        # refuse targets lacking the CID_Managed provenance — no delete call
        if not self._agent_is_cid_managed(existing):
            raise CidError(
                f'The agent {target_agent_id!r} exists but is not managed by Cloud Intelligence '
                'Dashboards (no CID_Managed provenance). Refusing to delete it. Please remove it '
                'manually if that is really what you want.'
            )
        agent_space_arns = [str(arn) for arn in existing.get('Spaces') or []]
        space_ids = self._agent_space_ids(definition, agent_space_arns)
        # confirmation via yes/no parameter defaulting to 'no'; -y/--yes confirms
        if not get_yesno_parameter(
                param_name='confirm-delete',
                message=f'Delete the agent {target_agent_id!r}? (no dashboard will be deleted)',
                default='no'):
            cid_print(f'Deletion of agent <BOLD>{target_agent_id}<END> was not confirmed. Nothing was deleted.')
            return target_agent_id

        # the Agent helper retries through ConflictException while the agent settles
        # and only calls DeleteAgent — no dashboard is ever deleted
        self.agent.delete(target_agent_id)
        cid_print(f'Agent <BOLD>{target_agent_id}<END> deleted. No dashboard was deleted.')
        self.track('deleted', target_agent_id)

        retained_space_ids = set(space_ids)
        if get_parameters().get('delete-space'):
            for space_id in sorted(space_ids):
                if self._delete_agent_space(space_id, target_agent_id):
                    retained_space_ids.discard(space_id)
        return target_agent_id

    def _space_override_removals(self, space_id: str, deployed_arns_by_id: dict, current_definition: dict=None):
        """Resources ``--on-drift override`` removes from an existing CID-managed Space
        (ARN -> type): empty for a missing Space, None for a Space that is not CID-managed
        (its contents are never synced)."""
        space = self.space.get(space_id)
        if space is None:
            return {}
        if not self._space_is_cid_managed(space):
            return None
        desired = self._space_desired_arns(space_id, deployed_arns_by_id, current_definition=current_definition)
        return compute_space_removals(self.space.current_resources(space_id), desired)

    def _agent_space_ids(self, definition: dict, space_arns: list) -> set:
        """Space ids of an agent: catalog `dependsOn.spaces` + the deployed Spaces list."""
        space_ids = set()
        for space_key in ((definition or {}).get('dependsOn') or {}).get('spaces') or []:
            space_ids.add(derive_space_id(space_key))
        for arn in space_arns or []:
            if ':space/' in str(arn):
                space_ids.add(str(arn).split(':space/', 1)[1])
        return space_ids

    def _space_dependent_agents(self, space_id: str, exclude_agent_id: str) -> list:
        """Other deployed, CID-managed catalog agents that reference the Space.

        A reference is either the deployed agent's Spaces list containing the Space or
        the agent's catalog `dependsOn.spaces` deriving to the same space id.
        """
        dependents = []
        for key, entry in (self.resources.get('agents') or {}).items():
            entry = entry if isinstance(entry, dict) else {}
            other_id = entry.get('agentId') or key
            if other_id == exclude_agent_id:
                continue
            try:
                other = self.agent.get(other_id)
            except Exception as exc:
                logger.debug(f'DescribeAgent failed for {other_id!r} ({exc}). Skipping its dependency check.')
                continue
            if other is None or not self._agent_is_cid_managed(other):
                continue
            catalog_refs = {
                derive_space_id(space_key)
                for space_key in (entry.get('dependsOn') or {}).get('spaces') or []
            }
            deployed_refs = {
                str(arn).split(':space/', 1)[1]
                for arn in other.get('Spaces') or [] if ':space/' in str(arn)
            }
            if space_id in catalog_refs or space_id in deployed_refs:
                dependents.append(other_id)
        return dependents

    def _delete_agent_space(self, space_id: str, deleted_agent_id: str) -> bool:
        """Delete the agent's Space unless another CID-managed agent references it.

        Never deletes a Space lacking the CID_Managed provenance (delete-agent manages
        CID-managed resources only). Returns True when the Space is gone (deleted or
        already absent), False when it is retained.
        """
        space = self.space.get(space_id)
        if space is None:
            logger.debug(f'Space {space_id!r} does not exist. Nothing to delete.')
            return True
        dependents = self._space_dependent_agents(space_id, deleted_agent_id)
        if dependents:
            cid_print(
                f'Space <BOLD>{space_id}<END> is retained: it is still referenced by the '
                f"CID-managed agent(s) {', '.join(sorted(dependents))}."
            )
            return False
        if not self._space_is_cid_managed(space):
            cid_print(
                f'Space <BOLD>{space_id}<END> is retained: it is not managed by Cloud Intelligence '
                'Dashboards (no CID_Managed provenance).'
            )
            return False
        self.space.delete(space_id)
        cid_print(f'Space <BOLD>{space_id}<END> deleted.')
        return True

    def _agent_apply_flow(self, agent_id: str=None, space_name: str=None, mode: str='create'):
        """Shared create-agent / update-agent flow; ``mode`` gates the existing-agent branch."""
        self.load_agentic_catalog()
        # 1. DETECT-AND-REQUIRE Enterprise subscription, read-only
        self._verify_enterprise_subscription()
        # 2-3. resolve the agent id (picker when absent) and its definition
        agent_key, definition = self._resolve_agent_definition(agent_id)
        # 5. cap validation before any CreateAgent call
        validate_caps(definition)
        # 6. read-only dependency pre-flight — NEVER deploys. Runs before anything that can
        # create or modify a resource (including the owner group): when none of the Space's
        # dashboards is deployed, the command stops here. The summary (Space, dashboards,
        # datasets) is also always shown before any prompt.
        space_name = space_name or get_parameters().get('space-name') or get_parameters().get('space')
        dependencies = self._preflight_agent_dependencies(definition, space_name=space_name)
        # 7. owner principal: registered QuickSight user with Author Pro
        principal_arn = self._resolve_quicksight_principal()

        # 6b. existing-agent gate — BEFORE any create or modify call.
        target_agent_id = definition['agentId']
        existing_agent = self.agent.get(target_agent_id)
        if existing_agent is not None and not self._agent_is_cid_managed(existing_agent):
            raise CidError(
                f'An agent with id {target_agent_id!r} already exists but was not created by '
                'Cloud Intelligence Dashboards. Refusing to modify it. Please rename the catalog '
                'agent id or remove the existing agent, then re-run.'
            )
        override = self._agent_override()
        drift = (describe_drift(definition, existing_agent, override=override)
                 if existing_agent is not None else [])
        if mode == 'update' and existing_agent is None:
            raise CidError(
                f'Agent {target_agent_id!r} is not deployed, so there is nothing to update. '
                f'Run cid-cmd create-agent --agent-id {agent_key} first.'
            )
        # --on-drift override: plan (read-only) what would be detached or removed
        plan = self._agent_override_plan(definition, existing_agent, space_name, dependencies) if override else None
        if mode == 'create' and existing_agent is not None:
            drift_note = (f'It differs from the catalog in: <BOLD>{", ".join(drift)}<END>.'
                          if drift else 'Its managed fields match the catalog.')
            cid_print(f'Agent <BOLD>{target_agent_id}<END> is already deployed. {drift_note}')
            if not isatty() and not getattr(self, 'all_yes', False) and get_parameters().get('update') is None:
                raise CidError(
                    f'Agent {target_agent_id!r} is already deployed. Pass --update yes to update it '
                    f'to the catalog, or run cid-cmd update-agent --agent-id {agent_key}.'
                )
            cid_print(self._agent_update_scope_note(plan, drifted=bool(drift)))
            if not get_yesno_parameter(
                    param_name='update',
                    message=f'Update agent {target_agent_id} to match the catalog?',
                    default='no'):
                cid_print(f'No changes made. Run <BOLD>cid-cmd update-agent --agent-id {agent_key}<END> when ready.')
                return target_agent_id
        elif mode == 'create':
            # new agent: confirm after the summary (the picker shows no dependencies)
            self._print_shared_space_note(definition, space_name, target_agent_id)
            if self._override_plan_has_changes(plan):
                cid_print(self._agent_update_scope_note(plan, drifted=False))
            if isatty() and not get_yesno_parameter(
                    param_name='confirm-create',
                    message=f'Create agent {target_agent_id}?',
                    default='yes'):
                cid_print('No changes made.')
                return target_agent_id
        if mode == 'update' and (drift or self._override_plan_has_changes(plan)):
            if drift:
                cid_print(f'Agent <BOLD>{target_agent_id}<END> differs from the catalog in: '
                          f'<BOLD>{", ".join(drift)}<END>.')
            cid_print(self._agent_update_scope_note(plan, drifted=bool(drift)))
            if not isatty() and not getattr(self, 'all_yes', False) and get_parameters().get('confirm-update') is None:
                # unattended update-agent: the user asked for the update explicitly
                logger.info('Unattended mode: applying the update without a prompt.')
            elif not get_yesno_parameter(
                    param_name='confirm-update',
                    message=f'Update agent {target_agent_id}?',
                    default='yes'):
                cid_print('No changes made.')
                return target_agent_id

        repair = bool(get_parameters().get('repair'))
        access_denied = self.space.client.exceptions.AccessDeniedException
        verb = 'Updating' if mode == 'update' else 'Creating'
        cid_print(f"\n{verb} agent <BOLD>{definition.get('name') or target_agent_id}<END>...")
        try:
            # 8. resolve the target Space
            space_id, space_arn = self._resolve_agent_space(definition, space_name, principal_arn)
            # 9. add missing resources for the PRESENT dashboards only
            self.space.update_resources(space_id, dependencies['dashboard_arns'], resource_type='DASHBOARD')
            # dataset knowledge: attach only PRESENT dataset ARNs, never create
            if dependencies['dataset_arns']:
                self.space.update_resources(space_id, dependencies['dataset_arns'], resource_type='DATA_SET')
            dashboards_count = len(dependencies['dashboard_arns'])
            datasets_count = len(dependencies['dataset_arns'])
            knowledge = f"{dashboards_count} dashboard{'s' if dashboards_count != 1 else ''}"
            if datasets_count:
                knowledge += f", {datasets_count} dataset{'s' if datasets_count != 1 else ''}"
            cid_print(f'\tKnowledge: {knowledge} linked')
            # 10. optional pre-existing knowledge bases; never create a KB
            self._attach_knowledge_bases(space_id, dependencies['knowledge_base_arns'])
            # 10b. --on-drift override: remove what the catalog config of the Space no longer needs
            if plan and plan['space_resources'] and plan['space_id'] == space_id:
                self.space.remove_resources(space_id, plan['space_resources'])
                cid_print(f"\tKnowledge: {len(plan['space_resources'])} resource(s) removed")
            # 11. create-or-update (existence and provenance were gated in step 6b)
            try:
                result = self.agent.create_or_update(definition, [space_arn], sync_spaces=override,
                                                     sync_connectors=override)
            except access_denied:
                raise  # no further create/modify (incl. cleanup) after AccessDenied
            except (CidError, CidCritical):
                raise
            except Exception:
                if existing_agent is None:
                    # cleanup-on-failure: best-effort delete of the partially created agent,
                    # then re-raise; delete retries through ConflictException
                    try:
                        self.agent.delete(target_agent_id)
                    except Exception as cleanup_exc:
                        logger.warning(f'Cleanup of partially created agent {target_agent_id!r} failed: {cleanup_exc}')
                raise
            if result.get('action') == 'created' and result.get('arn'):
                # dual provenance write: tag (tolerated failure) + Description marker
                self._write_provenance('agent', target_agent_id, result['arn'])
            action_word = {'created': 'created', 'updated': 'updated', 'unchanged': 'up to date'}.get(result.get('action'), 'ready')
            cid_print(f'\tAgent: {action_word} ({target_agent_id})')
            if result.get('action') in ('created', 'updated'):
                self.track(result['action'], target_agent_id)  # agent id sent in the existing payload
            # --repair (update-agent): detach and re-attach the Space links so the
            # service rewrites them. Recovers an agent whose Space shows as
            # unavailable although it describes as healthy.
            if repair and mode == 'update':
                if self.agent.repair_space_associations(target_agent_id):
                    cid_print(f'Space links of agent <BOLD>{target_agent_id}<END> rewritten (detach + re-attach).')
            # 12. owner grant: the 5-action bundle as one set
            self.agent.grant_owner(target_agent_id, principal_arn)
            # 13. PUBLISHED lifecycle: wait for ACTIVE; fail fast on FAILED
            if str(definition.get('lifecycle') or '').upper() == 'PUBLISHED':
                changed = result.get('action') != 'unchanged'
                if changed:
                    cid_print('\tStatus: waiting to become ACTIVE')
                wait_started = time.time()
                self.agent.wait_active(target_agent_id)
                cid_print(f'\tStatus: ACTIVE ({int(time.time() - wait_started)}s)' if changed
                          else '\tStatus: ACTIVE')
        except access_denied as exc:
            operation = getattr(exc, 'operation_name', None) or 'a Quick Suite generative-AI operation'
            raise CidCritical(
                f'Access denied calling {operation}. The caller is missing the QuickSight permission '
                f'quicksight:{operation}. No further create or modify calls were made. Please grant the '
                'missing permission and re-run.'
            ) from exc

        # 14. print the console URL
        console_url = build_console_url(
            self.base.account_id, self.base.region, self.base.partition, self.base.domain,
            'agent', target_agent_id,
        )
        action = {'created': 'created', 'updated': 'updated', 'unchanged': 'up to date'}.get(result.get('action'), 'ready')
        cid_print(
            f"\n#######\n####### Congratulations!\n"
            f"####### Agent '{definition.get('name')}' ({target_agent_id}) is {action} "
            f"and available at: {console_url}\n#######\n"
        )
        return target_agent_id

    @staticmethod
    def _agent_override() -> bool:
        """True for ``--on-drift override`` (exact sync); ``show`` (default) is additive."""
        on_drift = str(get_parameters().get('on-drift') or 'show').lower()
        if on_drift not in ('show', 'override'):
            raise CidError(f"Invalid --on-drift value {on_drift!r}. Use 'show' or 'override'.")
        return on_drift == 'override'

    @staticmethod
    def _override_plan_has_changes(plan) -> bool:
        """True when an override plan would detach or remove anything."""
        return bool(plan and (plan['spaces'] or plan['connectors'] or plan['space_resources']))

    def _agent_update_scope_note(self, plan=None, drifted: bool=True) -> str:
        """Explain what an agent update overwrites, keeps or removes.

        Without a plan (default ``--on-drift show``) anything added outside the catalog
        is kept; with a plan (``--on-drift override``) the exact removals are listed.
        """
        note = 'Console changes to the fields above are overwritten. ' if drifted else ''
        if plan is None:
            return note + ('Anything else added outside the catalog is kept (Spaces, action connectors, '
                           'Space dashboards and datasets). Use --on-drift override to remove it.')
        removals = []
        if plan['spaces']:
            removals.append('detach Spaces: ' + ', '.join(self._resource_label(arn) for arn in plan['spaces']))
        if plan['connectors']:
            removals.append('detach action connectors: ' + ', '.join(self._resource_label(arn) for arn in plan['connectors']))
        if plan['space_resources']:
            removals.append(f"remove from Space {plan['space_id']}: " + ', '.join(
                self._resource_label(arn, resource_type) for arn, resource_type in sorted(plan['space_resources'].items())))
        if plan.get('space_not_managed'):
            note += (f"Space {plan['space_id']} is not CID-managed, so its resources are left untouched. ")
        if not removals:
            return note + '--on-drift override: nothing to detach or remove.'
        return note + '<YELLOW>--on-drift override will<END> ' + '; '.join(removals) + '.'

    def _resource_label(self, arn: str, resource_type: str=None) -> str:
        """Readable label for a resource ARN: dataset name when known, else the resource id."""
        resource_id = str(arn).rsplit('/', 1)[-1]
        if resource_type == 'DATA_SET':
            dataset = (self.qs.datasets or {}).get(resource_id)
            if dataset is not None and getattr(dataset, 'name', None):
                return f'{dataset.name} (dataset)'
            return f'{resource_id} (dataset)'
        if resource_type == 'DASHBOARD':
            return f'{resource_id} (dashboard)'
        return resource_id

    def _print_shared_space_note(self, definition: dict, space_name: str, agent_id: str) -> None:
        """Tell the user when the new agent joins an existing catalog Space used by other agents."""
        if space_name:  # bring-your-own Space: reported when it is resolved
            return
        space_id, _, space = self._peek_agent_space(definition, space_name)
        if space is None:
            return
        others = self._space_dependent_agents(space_id, agent_id)
        if others:
            cid_print(f"Space <BOLD>{space_id}<END> already exists and is shared with: {', '.join(sorted(others))}.")

    def _peek_agent_space(self, definition: dict, space_name: str=None) -> tuple:
        """Read-only resolution of the agent's target Space; returns (space_id, space_arn, space).

        Mirrors :meth:`_resolve_agent_space` without creating or modifying anything;
        ``space`` is None when the Space does not exist yet.
        """
        if space_name:
            matching_ids = self.space.find_by_name(space_name)
            space_id = matching_ids[0] if len(matching_ids) == 1 else derive_space_id(space_name)
        else:
            depends_spaces = (definition.get('dependsOn') or {}).get('spaces') or []
            if not depends_spaces:
                return None, None, None
            space_id = derive_space_id(depends_spaces[0])
        space = self.space.get(space_id)
        return space_id, (space or {}).get('spaceArn') or self.space.arn(space_id), space

    def _space_desired_arns(self, space_id: str, deployed_arns_by_id: dict, current_definition: dict=None) -> set:
        """Resource ARNs the catalog definition of a Space resolves to (deployed resources only).

        A Space's contents are defined by the Space itself (``dependsOn`` of its catalog
        entry), so agents sharing it get exactly that content. A Space without a catalog
        entry (bring-your-own ``--space``) resolves to the catalog Space of the agent
        being created or updated (``current_definition``).
        """
        depends_on = None
        for space_key, space_definition in (self.resources.get('spaces') or {}).items():
            if derive_space_id(space_key) == space_id and isinstance(space_definition, dict):
                depends_on = space_definition.get('dependsOn') or {}
                break
        if depends_on is None:
            depends_on = self._agent_space_definition(current_definition or {}).get('dependsOn') or {}
        desired = set()
        for dashboard_id in depends_on.get('dashboards') or []:
            if dashboard_id in deployed_arns_by_id:
                desired.add(deployed_arns_by_id[dashboard_id])
        for dataset_key in depends_on.get('datasets') or []:
            arn = self._find_deployed_dataset_arn(dataset_key)
            if arn:
                desired.add(arn)
        for arn in depends_on.get('knowledgeBases') or []:
            desired.add(arn)
        return desired

    def _agent_override_plan(self, definition: dict, existing_agent: dict, space_name: str,
                             dependencies: dict) -> dict:
        """Read-only plan of what ``--on-drift override`` detaches from the agent and
        removes from its Space. Space contents are only synced for CID-managed Spaces."""
        space_id, space_arn, _ = self._peek_agent_space(definition, space_name)
        plan = {'space_id': space_id, 'spaces': [], 'connectors': [], 'space_resources': {}}
        if existing_agent is not None:
            plan['spaces'] = sorted(set(existing_agent.get('Spaces') or ()) - {space_arn})
            desired_connectors = set((definition.get('dependsOn') or {}).get('actionConnectors') or ())
            plan['connectors'] = sorted(set(existing_agent.get('ActionConnectors') or ()) - desired_connectors)
        if space_id:
            removals = self._space_override_removals(
                space_id, dependencies['deployed_dashboard_arns_by_id'], current_definition=definition)
            plan['space_resources'] = removals or {}
            plan['space_not_managed'] = removals is None
        return plan

    def _verify_enterprise_subscription(self):
        """Read-only DETECT-AND-REQUIRE of QuickSight Enterprise.

        NEVER activates, enables, or modifies a subscription (unlike Cid.ensure_subscription,
        which offers activation) — billing is the customer's explicit action.
        """
        try:
            self.qs.ensure_subscription()
        except CidCritical as exc:
            raise CidCritical(
                'An active Amazon QuickSight Enterprise subscription is required to create Quick '
                'Agents, and this command will not activate one. Please enable the QuickSight '
                'Enterprise edition from the QuickSight console (https://quicksight.aws.amazon.com/) '
                f'and re-run. Detection result: {exc}'
            ) from exc

    def _deployed_agent_ids(self) -> set:
        """Enumerate deployed agent ids via read-only ListAgents (enumeration only).

        ListAgents omits PREVIEW/FAILED agents, so this is used solely for the picker's
        check indicator; existence checks use DescribeAgent. Failures are
        tolerated: the picker then simply shows no check indicators.
        """
        ids = set()
        try:
            parameters = {'AwsAccountId': self.base.account_id}
            while True:
                response = self.agent.client.list_agents(**parameters)
                ids.update(summary['AgentId'] for summary in response.get('AgentSummaries') or [] if summary.get('AgentId'))
                if not response.get('NextToken'):
                    break
                parameters['NextToken'] = response['NextToken']
        except Exception as exc:
            logger.debug(f'ListAgents failed ({exc}). Deployed check indicators will be skipped.')
        return ids

    def _select_agent(self, message: str) -> str:
        """Agent picker in the dashboard-picker style (`` ✓[agentId] name``, ✓ = deployed via
        ListAgents, type-to-search); returns the catalog key. Dependencies are shown after
        selection by the pre-flight summary and in ``list-agents``."""
        listing = build_agent_listing(self.resources.get('agents') or {}, self._deployed_agent_ids())
        options = dict(zip(self._agent_labels(listing), (entry['key'] for entry in listing)))
        try:
            return get_parameter(param_name='agent-id', message=message, choices=options)
        except Exception as exc:
            # non-interactive with no value/default: name the missing input
            raise CidCritical(
                "Required input 'agent-id' has no supplied value, stored default, or "
                'fallback in a non-interactive environment. Please provide --agent-id.'
            ) from exc

    @staticmethod
    def _agent_labels(listing: list, displays: list=None) -> list:
        """Picker/list labels: `` ✓[agentId] name`` padded to one column, then the agent summary,
        cut to the terminal width."""
        displays = displays or [entry['display'] for entry in listing]
        width = min(shutil.get_terminal_size((120, 24)).columns - 4, 160)
        column = max((len(display) for display in displays), default=0)
        labels = []
        for display, entry in zip(displays, listing):
            label = f"{display.ljust(column)}  {entry['summary']}".rstrip() if entry.get('summary') else display
            labels.append(label if len(label) <= width else label[:width - 1] + '…')
        return labels

    def _resolve_agent_definition(self, agent_id: str=None) -> tuple:
        """Resolve the agent id (picker when absent) and load its definition.

        Returns (agent_key, definition) where definition is a copy of the catalog entry
        with the persona loaded inline.
        """
        agents_catalog = self.resources.get('agents') or {}
        if not agents_catalog:
            raise CidError('No agents found in the catalog.')
        agent_key = agent_id or get_parameters().get('agent-id')
        if not agent_key:
            agent_key = self._select_agent('Please select an agent to create')
        # tolerate the deployed agentId as input in addition to the catalog key
        if agent_key not in agents_catalog:
            agent_key = next(
                (key for key, entry in agents_catalog.items() if (entry or {}).get('agentId') == agent_key),
                agent_key,
            )
        definition = self.get_definition('agent', name=agent_key)
        if not isinstance(definition, dict):
            raise CidError(
                f'Agent {agent_key!r} is not found in the catalog. '
                'Please check the agent id or provide the catalog that defines it.'
            )
        set_parameters({'agent-id': agent_key})
        definition = dict(definition)  # do not mutate the shared catalog entry
        definition['persona'] = self._load_agent_persona(definition)
        return agent_key, definition

    def _load_agent_persona(self, definition: dict) -> dict:
        """Load the persona for an agent definition (inline dict or resolved personaFile)."""
        persona = definition.get('persona')
        if isinstance(persona, dict):
            return persona
        persona_file = definition.get('personaFile')
        if not persona_file:
            return None  # validate_caps reports the missing required 'persona' field
        try:
            content = yaml.safe_load(self.load_text_file(persona_file, parent_source=definition.get('source')))
        except Exception as exc:
            raise CidError(f'Failed to load the persona file {persona_file!r}: {exc}') from exc
        if not isinstance(content, dict):
            raise CidError(f'Persona file {persona_file!r} must contain a mapping of the 5 persona fields.')
        return content

    def _resolve_quicksight_principal(self) -> str:
        """Owner principal, resolved like for dashboards (``qs.get_principal_arn``); a user
        principal must also carry the Author Pro entitlement to own Quick Agents."""
        principal_arn = self.qs.get_principal_arn()
        if ':user/' in str(principal_arn):
            user = self.qs.user or {}
            if user.get('Role') not in self.AGENT_AUTHOR_PRO_ROLES:
                raise CidCritical(
                    f"The QuickSight user {user.get('UserName', principal_arn)!r} has role {user.get('Role')!r}, "
                    'but creating Quick Agents requires the Author Pro entitlement. Please register or upgrade '
                    'the user as an Author Pro (or Admin Pro) user, or pass --quicksight-group, and re-run.'
                )
        return principal_arn

    def _agent_space_definition(self, definition: dict) -> dict:
        """The catalog definition of the agent's Space (``dependsOn.spaces[0]``), or {}."""
        spaces = (definition.get('dependsOn') or {}).get('spaces') or []
        space_definition = (self.resources.get('spaces') or {}).get(spaces[0]) if spaces else None
        return space_definition if isinstance(space_definition, dict) else {}

    def _collect_agent_dependency_keys(self, definition: dict) -> tuple:
        """Knowledge dependencies of an agent, read from its Space: (dashboard_ids, dataset_keys, knowledge_base_arns)."""
        misplaced = sorted(set(definition.get('dependsOn') or {}) & {'dashboards', 'optionalDashboards', 'datasets', 'knowledgeBases'})
        if misplaced:
            logger.warning(f"Agent {definition.get('agentId')!r} declares {', '.join(misplaced)} under dependsOn; "
                           'these belong to its Space definition and are ignored.')
        depends_on = self._agent_space_definition(definition).get('dependsOn') or {}
        if depends_on.get('optionalDashboards'):
            logger.warning("Space 'optionalDashboards' is not supported and is ignored; list every dashboard "
                           "under 'dashboards' (missing ones only produce a warning).")
        dashboards, datasets, knowledge_bases = [], [], []
        for key in depends_on.get('dashboards') or []:
            if key not in dashboards:
                dashboards.append(key)
        for key in depends_on.get('datasets') or []:
            if key not in datasets:
                datasets.append(key)
        for arn in depends_on.get('knowledgeBases') or []:
            if arn not in knowledge_bases:
                knowledge_bases.append(arn)
        return dashboards, datasets, knowledge_bases

    def _deployed_dashboard_arns_by_id(self) -> dict:
        """dashboardId -> Arn strictly from the read-only ListDashboards response."""
        deployed_arns_by_id = {}
        for summary in self.qs.list_dashboards():
            if summary.get('DashboardId') and summary.get('Arn'):
                deployed_arns_by_id[summary['DashboardId']] = summary['Arn']
        return deployed_arns_by_id

    def _deployed_dashboard_arns_or_none(self):
        """Tolerant variant for dependency annotations: None when ListDashboards
        fails, so the caller skips annotations and continues."""
        try:
            return self._deployed_dashboard_arns_by_id()
        except Exception as exc:
            logger.debug(f'ListDashboards failed ({exc}). Dependency annotations will be skipped.')
            return None

    def _present_dashboard_ids(self, dashboards: list, deployed_arns_by_id: dict) -> set:
        """Dependency dashboard ids that are deployed; ids unknown to the dashboards catalog are warned about."""
        known_ids = {(entry or {}).get('dashboardId') for entry in (self.resources.get('dashboards') or {}).values()}
        for dashboard_id in dashboards:
            if dashboard_id not in known_ids:
                # expected for CID dashboards deployed from their own resources file (e.g. cid-crcd);
                # typos in shipped catalogs are caught by the catalog tests
                logger.info(f'Dependency dashboard id {dashboard_id!r} is not in the dashboards catalog '
                            '(fine for dashboards deployed from another resources file).')
        return {dashboard_id for dashboard_id in dashboards if dashboard_id in deployed_arns_by_id}

    def _preflight_agent_dependencies(self, definition: dict, space_name: str=None) -> dict:
        """Read-only pre-flight: print the dependency summary, stop when no dashboard is deployed, and
        return the deployed dashboard/dataset ARNs to link. Never deploys anything."""
        dashboards, dataset_keys, knowledge_base_arns = self._collect_agent_dependency_keys(definition)
        deployed_arns_by_id = self._deployed_dashboard_arns_by_id()
        present_ids = self._present_dashboard_ids(dashboards, deployed_arns_by_id)
        classification = classify_dependencies(dashboards, present_ids)
        present_dashboard_arns = [deployed_arns_by_id[dashboard_id] for dashboard_id in classification['present']]
        # dataset knowledge presence via the existing read-only discovery
        present_dataset_arns, missing_datasets = [], []
        for key in dataset_keys:
            arn = self._find_deployed_dataset_arn(key)
            if arn:
                present_dataset_arns.append(arn)
            else:
                missing_datasets.append(key)

        spaces = (definition.get('dependsOn') or {}).get('spaces') or []
        space_label = space_name or (spaces[0] if spaces else None)
        header = f"Dependencies of agent <BOLD>{definition.get('name') or definition.get('agentId')}<END>"
        cid_print(header + (f' (Space <BOLD>{space_label}<END>):' if space_label else ':'))
        for summary_line in self._dependency_summary_lines(dashboards, present_ids):
            cid_print(summary_line)
        if dataset_keys:
            datasets_line = f'     datasets:   {len(present_dataset_arns)} of {len(dataset_keys)} found'
            if missing_datasets:
                datasets_line += f' (missing: {", ".join(missing_datasets)})'
            cid_print(datasets_line)
        if not classification['present']:
            raise CidError(
                'None of the dashboards this agent needs are deployed in this account and region. '
                f'Nothing was created. Deploy the dashboards first — see {self.CID_DASHBOARDS_GUIDANCE}.'
            )
        if classification['missing'] or missing_datasets:
            cid_print(
                '     <YELLOW>✗ = not deployed. Missing items are optional:<END> the agent works with what is '
                'deployed, and you can add the rest later: deploy them, then re-run this command.\n'
                f'     Deployment: {self.CID_DASHBOARDS_GUIDANCE}'
            )
        return {
            'classification': classification,
            'dashboard_arns': present_dashboard_arns,
            'dataset_arns': present_dataset_arns,
            'knowledge_base_arns': knowledge_base_arns,
            'deployed_dashboard_arns_by_id': deployed_arns_by_id,
        }

    def _find_deployed_dataset_arn(self, dataset_key: str) -> str:
        """Resolve a dataset catalog key to a deployed dataset ARN via read-only discovery."""
        datasets = self.qs.datasets  # ListDataSets, or dashboard-based discovery on AccessDenied
        catalog_entry = (self.resources.get('datasets') or {}).get(dataset_key) or {}
        catalog_data = catalog_entry.get('data')
        catalog_dataset_id = catalog_data.get('DataSetId') if isinstance(catalog_data, dict) else None
        for candidate_id in (catalog_dataset_id, dataset_key):
            if candidate_id and candidate_id in datasets:
                return datasets[candidate_id].arn
        for dataset in datasets.values():
            if isinstance(dataset, Dataset) and dataset.name == dataset_key:
                return dataset.arn
        return None

    def _agent_is_cid_managed(self, agent: dict) -> bool:
        """Dual-mechanism CID_Managed provenance detection for an Agent."""
        return is_cid_managed(self.qs.get_tags(agent.get('Arn')) if agent.get('Arn') else {}, agent.get('Description'))

    def _space_is_cid_managed(self, space: dict) -> bool:
        """Dual-mechanism CID_Managed provenance detection for a Space (as returned by space.get)."""
        return is_cid_managed(self.qs.get_tags(space['spaceArn']), space.get('description'))

    def _write_provenance(self, kind: str, resource_id: str, arn: str) -> None:
        """Provenance tag (via qs.set_tags, tolerated failure) + description marker."""
        (self.space if kind == 'space' else self.agent).write_marker(resource_id)
        self.qs.set_tags(arn, **{CID_PROVENANCE_TAG_KEY: CID_PROVENANCE_TAG_VALUE})

    def _resolve_agent_space(self, definition: dict, space_name: str, principal_arn: str) -> tuple:
        """Resolve or create the target Space (catalog Space or --space name); returns (space_id, space_arn).
        A pre-existing Space that is not CID-managed is reused additively and never modified."""
        if space_name:  # bring-your-own Space
            matching_ids = self.space.find_by_name(space_name)
            if len(matching_ids) > 1:
                raise CidError(
                    f'Multiple Spaces match the name {space_name!r}: {", ".join(sorted(matching_ids))}. '
                    'Please disambiguate by renaming Spaces so the name is unique.'
                )
            if matching_ids:
                return self._reuse_existing_space(matching_ids[0], principal_arn)
            space_id = derive_space_id(space_name)  # deterministic id from the name
            name = space_name
            description = None
        else:
            depends_spaces = (definition.get('dependsOn') or {}).get('spaces') or []
            if not depends_spaces:
                raise CidError(
                    f"Agent {definition.get('agentId')!r} does not declare a Space under 'dependsOn.spaces' "
                    'and no --space was provided. Please declare a Space in the catalog or pass --space.'
                )
            space_key = depends_spaces[0]
            space_definition = self.get_definition('space', name=space_key) or {}
            space_id = derive_space_id(space_key)  # deterministic id from the catalog id
            name = space_definition.get('name') or space_key
            description = space_definition.get('description')
            if self.space.get(space_id) is not None:
                return self._reuse_existing_space(space_id, principal_arn, name=name, description=description)
        cid_print(f'\tSpace: creating <BOLD>{space_id}<END>')
        space_arn = self.space.create_or_update(space_id, name, description=description)
        self._write_provenance('space', space_id, space_arn)
        self.space.grant_owner(space_id, principal_arn)
        return space_id, space_arn

    def _reuse_existing_space(self, space_id: str, principal_arn: str, name: str=None, description: str=None) -> tuple:
        """Reuse an existing Space; returns (space_id, space_arn).

        CID-managed Spaces get the idempotent update + provenance + owner grant. Spaces
        lacking the CID_Managed provenance are reused strictly additively: no rename, no
        description rewrite, no tagging of the Space itself, and no permission change.
        """
        space = self.space.get(space_id) or {'spaceArn': self.space.arn(space_id)}
        space_arn = space['spaceArn']
        if self._space_is_cid_managed(space):
            cid_print(f'\tSpace: reusing <BOLD>{space_id}<END>')
            self.space.create_or_update(space_id, name or space.get('name') or space_id, description=description)
            self._write_provenance('space', space_id, space_arn)
            self.space.grant_owner(space_id, principal_arn)
        else:
            cid_print(
                f'\tSpace: reusing <BOLD>{space_id}<END> additively (not CID-managed: existing '
                'resources, name, description, tags and permissions are left untouched)'
            )
        return space_id, space_arn

    def _attach_knowledge_bases(self, space_id: str, knowledge_base_arns: list):
        """Attach pre-existing knowledge-base ARNs to the Space.

        Never creates a knowledge base or Research asset. Unresolvable
        references are warned about and skipped; attach failures are reported by the
        Space helper and the deployment continues with the dashboards.
        """
        resolvable = []
        for arn in knowledge_base_arns or []:
            if not str(arn).startswith('arn:'):
                cid_print(
                    f'<YELLOW>Warning:<END> the knowledge-base reference {arn!r} cannot be resolved '
                    '(not an ARN). Skipping it and continuing with the dashboards.'
                )
                continue
            resolvable.append(arn)
        if not resolvable:
            return
        failed = self.space.update_resources(space_id, resolvable, resource_type='KNOWLEDGE_BASE')
        if failed:
            cid_print(
                f'<YELLOW>Warning:<END> {len(failed)} knowledge-base resource(s) could not be attached '
                f'to Space {space_id!r}. Continuing with the dashboards.'
            )
