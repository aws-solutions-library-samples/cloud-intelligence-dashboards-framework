""" Quick Suite Space helper.

A **Space** is a Quick Suite (QuickSight) container of resources (dashboards, topics,
datasets, knowledge bases, action connectors) that a chat agent uses as its knowledge
layer. All Space operations live on the ``quicksight`` boto3 client
(``quicksight-2018-04-01``); there is no separate quicksuite client.

This helper is the thin AWS I/O layer over the pure set-math in
:mod:`cid.helpers.quicksight.agent_logic`.
"""
import time
import logging

from cid.base import CidBase
from cid.exceptions import CidError
from cid.helpers.quicksight.agent_logic import (
    CID_MANAGED_MARKER,
    CID_PROVENANCE_TAG_KEY,
    CID_PROVENANCE_TAG_VALUE,
    compute_space_additions,
    compute_stale_removals,
)

logger = logging.getLogger(__name__)

# The exact owner action set the Quick console applies on "Create space" (verified live).
# An API-created Space starts with Permissions: [] and the creating principal is NOT
# auto-granted owner rights, so this helper must grant this full 16-action set or the
# customer cannot open/use the Space in the console.
# NOTE: quicksight:UpdateSpaceResources is IAM-gated on the caller and is NOT a grantable
# space permission — it never appears in this set.
SPACE_OWNER_ACTIONS = [
    "quicksight:DescribeSpace", "quicksight:UpdateSpace", "quicksight:DeleteSpace",
    "quicksight:DescribeSpacePermissions", "quicksight:UpdateSpacePermissions",
    "quicksight:CreateDocument", "quicksight:GetDocument", "quicksight:ListDocument",
    "quicksight:DeleteDocument", "quicksight:GetArtifact", "quicksight:ListArtifactVersions",
    "quicksight:CreateSpaceFolder", "quicksight:UpdateSpaceFolder", "quicksight:DeleteSpaceFolder",
    "quicksight:ListSpaceFolderMembers", "quicksight:MoveSpaceFolderMember",
]  # 16 actions


def _first_key(dictionary, *keys, default=None):
    """Return the first non-None value among the given keys.

    The gen-AI Space APIs mix lowercase (``spaceArn``, ``name``) and PascalCase
    (``ResourceDetails``) keys across operations, so responses are read defensively.
    """
    for key in keys:
        value = (dictionary or {}).get(key)
        if value is not None:
            return value
    return default


class Space(CidBase):
    """AWS I/O helper for Quick Suite Spaces (create/read/update, permissions, provenance)."""

    def __init__(self, session, resources=None):
        super().__init__(session)
        self.client = self.session.client('quicksight')
        self.resources = resources or {}

    def get(self, space_id) -> dict:
        """Read a Space, tolerating absence.

        :param space_id: the SpaceId to describe
        :returns: the Space object dict (lowercase fields: ``name``, ``description``,
            ``resources``, ``createdAt``, ...) or None when the Space does not exist
        """
        try:
            response = self.client.describe_space(
                AwsAccountId=self.account_id,
                SpaceId=space_id,
            )
        except self.client.exceptions.ResourceNotFoundException:
            logger.debug(f'Space {space_id!r} not found')
            return None
        return _first_key(response, 'Space', 'space', default=response)

    def create_or_update(self, space_id, name, description=None) -> str:
        """Idempotently create the Space or update its name/description.

        :param space_id: unique SpaceId (``[0-9a-zA-Z-_=.+]+``)
        :param name: display name (1-1000 chars)
        :param description: optional description (<=1000 chars)
        :returns: the space ARN
        """
        parameters = {
            'AwsAccountId': self.account_id,
            'SpaceId': space_id,
            'Name': name,
        }
        if description is not None:
            parameters['Description'] = description
        existing = self.get(space_id)
        if existing is None:
            logger.debug(f'Creating space {space_id!r}')
            try:
                response = self.client.create_space(**parameters)
            except self.client.exceptions.ResourceExistsException:
                # lost a race with a concurrent create; treat as existing
                logger.debug(f'Space {space_id!r} already exists')
                response = self.client.update_space(**parameters)
            logger.info(f'Space {space_id!r} created')
        else:
            logger.debug(f'Updating space {space_id!r}')
            response = self.client.update_space(**parameters)
            logger.info(f'Space {space_id!r} updated')
        arn = _first_key(response, 'spaceArn', 'SpaceArn')
        if not arn:
            # verified live ARN format: arn:<partition>:quicksight:<region>:<account>:space/<spaceId>
            arn = f'arn:{self.partition}:quicksight:{self.region}:{self.account_id}:space/{space_id}'
        return arn

    def current_resource_arns(self, space_id) -> set:
        """Enumerate the resource ARNs currently attached to a Space.

        Each ListSpaceResources item has the shape
        ``{'ResourceType': ..., 'ResourceDetails': {'resourceArn': ...}}`` (verified live).

        :param space_id: the SpaceId to enumerate
        :returns: set of resource ARN strings
        """
        arns = set()
        parameters = {
            'AwsAccountId': self.account_id,
            'SpaceId': space_id,
        }
        while True:
            response = self.client.list_space_resources(**parameters)
            items = _first_key(response, 'Resources', 'resources', 'SpaceResources', default=[])
            for item in items or []:
                details = _first_key(item, 'ResourceDetails', 'resourceDetails', default={})
                arn = _first_key(details, 'resourceArn', 'ResourceArn')
                if arn:
                    arns.add(arn)
            next_token = _first_key(response, 'NextToken', 'nextToken')
            if not next_token:
                break
            parameters['NextToken'] = next_token
        return arns

    def grant_owner(self, space_id, principal_arn) -> None:
        """Grant the full 16-action owner set to a principal.

        Must be the exact :data:`SPACE_OWNER_ACTIONS` set the console applies on
        "Create space" — the minimal 5-action set leaves the Space unusable
        (console popup "can't see documents").

        :param space_id: the SpaceId to share
        :param principal_arn: QuickSight user/group ARN to grant owner rights to
        """
        self.client.update_space_permissions(
            AwsAccountId=self.account_id,
            SpaceId=space_id,
            GrantPermissions=[{
                'Principal': principal_arn,
                'Actions': list(SPACE_OWNER_ACTIONS),
            }],
        )
        logger.info(f'Granted space owner permissions on {space_id!r} to {principal_arn!r}')

    def find_by_name(self, name) -> list:
        """Resolve a Space display name to space ids via SearchSpaces.

        Uses the official ``SpaceQuicksightSearchFilter`` shape with LOWERCASE keys
        (``name``/``operator``/``value``); names ``SPACE_ID | SPACE_NAME``; operators
        ``STRING_EQUALS | STRING_LIKE | NUMBER_RANGE``.

        :param name: the exact Space display name to search for
        :returns: list of matching space ids (possibly empty; multiple on name collision)
        """
        space_ids = []
        parameters = {
            'AwsAccountId': self.account_id,
            'Filters': [{
                'name': 'SPACE_NAME',
                'operator': 'STRING_EQUALS',
                'value': name,
            }],
        }
        while True:
            response = self.client.search_spaces(**parameters)
            summaries = _first_key(response, 'SpaceSummaries', 'spaceSummaries', 'Spaces', default=[])
            for summary in summaries or []:
                space_id = _first_key(summary, 'spaceId', 'SpaceId')
                if space_id:
                    space_ids.append(space_id)
            next_token = _first_key(response, 'NextToken', 'nextToken')
            if not next_token:
                break
            parameters['NextToken'] = next_token
        return space_ids

    def update_resources(self, space_id, desired_arns, resource_type='DASHBOARD',
                            remove_stale=False, managed_arns=None) -> list:
        """Add any missing resources to the Space so it contains the desired ARN set.

        The update is strictly additive: only ARNs not already
        attached are added (de-duplicated by ARN via
        :func:`~cid.helpers.quicksight.agent_logic.compute_space_additions`), and
        pre-existing resources are never removed or reconfigured. Removal is opt-in
: when ``remove_stale`` is truthy, only CID-managed
        (``managed_arns``) resources no longer referenced by ``desired_arns`` are
        removed, via
        :func:`~cid.helpers.quicksight.agent_logic.compute_stale_removals`.

        The caller MUST pre-flight resource existence (e.g. ListDashboards):
        UpdateSpaceResources does not validate resource existence, so
        ``FailedResourceOperations`` only surfaces permission/format failures. Each
        failed operation is reported via a logger warning and the update continues
        with the resources that succeeded.

        :param space_id: the SpaceId to update
        :param desired_arns: iterable of resource ARNs the Space should contain
            (only ARNs confirmed present upstream)
        :param resource_type: SpaceResourceOperation ResourceType
            (``DASHBOARD | DATA_SET | TOPIC | KNOWLEDGE_BASE | ACTION_CONNECTOR``);
            ``DATA_SET`` is used for dataset knowledge attachment
        :param remove_stale: the ``--cleanup-space`` flag; falsy means
            nothing is ever removed
        :param managed_arns: iterable of ARNs known to be CID-managed; only these are
            removal candidates when ``remove_stale`` is truthy
        :returns: the ``FailedResourceOperations`` list from the response (empty on
            full success or when the call was a no-op)
        """
        current_arns = self.current_resource_arns(space_id)
        to_add = compute_space_additions(current_arns, desired_arns)
        to_remove = compute_stale_removals(current_arns, desired_arns, managed_arns, remove_stale)
        if not to_add and not to_remove:
            logger.debug(f'Space {space_id!r} resources already up to date, nothing to change')
            return []
        parameters = {
            'AwsAccountId': self.account_id,
            'SpaceId': space_id,
        }
        if to_add:
            parameters['AddResources'] = [
                {'ResourceType': resource_type, 'ResourceDetails': {'resourceArn': arn}}
                for arn in sorted(to_add)
            ]
        if to_remove:
            parameters['RemoveResources'] = [
                {'ResourceType': resource_type, 'ResourceDetails': {'resourceArn': arn}}
                for arn in sorted(to_remove)
            ]
        logger.debug(
            f'Updating space {space_id!r} resources ({resource_type}): '
            f'adding {len(to_add)}, removing {len(to_remove)}'
        )
        response = self.client.update_space_resources(**parameters)
        failed = _first_key(response, 'FailedResourceOperations', 'failedResourceOperations', default=[]) or []
        for failure in failed:
            details = _first_key(failure, 'ResourceDetails', 'resourceDetails', default={})
            arn = _first_key(details, 'resourceArn', 'ResourceArn', default='<unknown>')
            error_message = _first_key(failure, 'ErrorMessage', 'errorMessage', default='<no error message>')
            failed_type = _first_key(failure, 'ResourceType', 'resourceType', default=resource_type)
            logger.warning(
                f'Failed to update {failed_type} resource {arn!r} '
                f'in space {space_id!r}: {error_message}. Continuing with the remaining resources.'
            )
        succeeded = len(to_add) + len(to_remove) - len(failed)
        logger.info(
            f'Space {space_id!r} resources updated ({resource_type}): '
            f'{succeeded} succeeded, {len(failed)} failed'
        )
        return failed

    def write_provenance(self, space_arn, space_id, timeout=300, interval=5) -> None:
        """Write the dual CID_Managed provenance signature onto a Space.

        (a) TagResource with the CID provenance tag — LOG-AND-CONTINUE on failure,
        mirroring ``QuickSight.set_tags`` (the tag write is best-effort because
        TagResource can be denied independently of Space permissions), and
        (b) ensure the CID-managed marker is present in the Space Description via
        UpdateSpace — the always-written signal ``is_cid_managed`` falls back on.

        Defensively mirrors ``Agent.write_provenance``: if UpdateSpace raises
        ConflictException while the Space status settles, the marker write is retried
        every ``interval`` seconds up to ``timeout`` seconds (re-describing on each
        attempt to carry the current Name through). If the conflict never settles,
        a WARNING is logged and the write is abandoned instead of crashing the
        command — the provenance tag (a) was already written and ``is_cid_managed``
        accepts either mechanism.

        :param space_arn: the Space ARN to tag
        :param space_id: the SpaceId whose Description carries the marker
        :param timeout: maximum total seconds to retry through ConflictException (default 300)
        :param interval: seconds between retries (default 5)
        """
        # (a) provenance tag: tolerated failure, mirroring set_tags
        try:
            self.client.tag_resource(
                ResourceArn=space_arn,
                Tags=[{'Key': CID_PROVENANCE_TAG_KEY, 'Value': CID_PROVENANCE_TAG_VALUE}],
            )
            logger.debug(f'Tagged space {space_id!r} with {CID_PROVENANCE_TAG_KEY}')
        except self.client.exceptions.AccessDeniedException:
            logger.debug(f'Cannot tag {space_arn} (AccessDenied).')
        except self.client.exceptions.ClientError as exc:
            logger.debug(f'Cannot tag {space_arn} ({exc}).')

        # (b) CID-managed marker in the Description: the always-written provenance
        # signal, retried through ConflictException while the Space status settles.
        elapsed = 0
        while True:
            space = self.get(space_id)
            if space is None:
                raise CidError(f'Cannot write provenance: space {space_id!r} does not exist.')
            description = str(_first_key(space, 'description', 'Description', default='') or '')
            if CID_MANAGED_MARKER in description:
                logger.debug(f'Space {space_id!r} description already carries the CID-managed marker')
                return
            name = _first_key(space, 'name', 'Name', default=space_id)
            new_description = f'{description} {CID_MANAGED_MARKER}'.strip() if description else CID_MANAGED_MARKER
            try:
                self.client.update_space(
                    AwsAccountId=self.account_id,
                    SpaceId=space_id,
                    Name=name,
                    Description=new_description,
                )
                logger.debug(f'Wrote CID-managed marker into the description of space {space_id!r}')
                return
            except self.client.exceptions.ConflictException as exc:
                if elapsed >= timeout:
                    # Do not crash the command for a provenance-marker write: the
                    # provenance tag (a) was already written and is_cid_managed
                    # accepts either mechanism.
                    logger.warning(
                        f'Could not write the Description provenance marker on space {space_id!r}: '
                        f'the space was still busy (ConflictException) after {timeout} seconds. '
                        f'The CID provenance tag was already written, so CID-managed detection '
                        f'is unaffected. Last error: {exc}'
                    )
                    return
                logger.debug(
                    f'Space {space_id!r} provenance marker write hit ConflictException; '
                    f'retrying in {interval}s ({elapsed}/{timeout}s elapsed).'
                )
                time.sleep(interval)
                elapsed += interval
