""" Quick Space helper.

A Space is a Quick container of resources (dashboards, datasets, knowledge bases, ...)
that a chat agent uses as its knowledge layer. All Space operations live on the
``quicksight`` boto3 client.
"""
import logging

from cid.base import CidBase
from cid.helpers.quicksight.agent_logic import CID_MANAGED_MARKER, compute_space_additions, retry_on_conflict

logger = logging.getLogger(__name__)

# The owner action set the Quick console applies on "Create space". An API-created Space
# starts with no permissions, so the full set must be granted or the Space is unusable.
SPACE_OWNER_ACTIONS = [
    "quicksight:DescribeSpace", "quicksight:UpdateSpace", "quicksight:DeleteSpace",
    "quicksight:DescribeSpacePermissions", "quicksight:UpdateSpacePermissions",
    "quicksight:CreateDocument", "quicksight:GetDocument", "quicksight:ListDocument",
    "quicksight:DeleteDocument", "quicksight:GetArtifact", "quicksight:ListArtifactVersions",
    "quicksight:CreateSpaceFolder", "quicksight:UpdateSpaceFolder", "quicksight:DeleteSpaceFolder",
    "quicksight:ListSpaceFolderMembers", "quicksight:MoveSpaceFolderMember",
]


class Space(CidBase):
    """ Quick Space operations (create/read/update/delete, resources, permissions) """

    def __init__(self, session, resources=None):
        super().__init__(session)
        self.client = self.session.client('quicksight')
        self.resources = resources or {}

    def arn(self, space_id: str) -> str:
        """ The Space ARN """
        return f'arn:{self.partition}:quicksight:{self.region}:{self.account_id}:space/{space_id}'

    def get(self, space_id: str) -> dict:
        """ Describe a Space; returns its fields plus ``spaceArn``, or None when it does not exist """
        try:
            response = self.client.describe_space(AwsAccountId=self.account_id, SpaceId=space_id)
        except self.client.exceptions.ResourceNotFoundException:
            logger.debug(f'Space {space_id!r} not found')
            return None
        return dict(response.get('Space') or {}, spaceArn=response.get('spaceArn') or self.arn(space_id))

    def create_or_update(self, space_id: str, name: str, description: str=None) -> str:
        """ Create the Space or update its name/description; returns the Space ARN """
        parameters = {'AwsAccountId': self.account_id, 'SpaceId': space_id, 'Name': name}
        if description is not None:
            parameters['Description'] = description
        if self.get(space_id) is None:
            try:
                response = self.client.create_space(**parameters)
                logger.info(f'Space {space_id!r} created')
            except self.client.exceptions.ResourceExistsException:  # lost a race with a concurrent create
                response = self.client.update_space(**parameters)
        else:
            response = self.client.update_space(**parameters)
            logger.info(f'Space {space_id!r} updated')
        return response.get('spaceArn') or self.arn(space_id)

    def delete(self, space_id: str) -> None:
        """ Delete a Space; a missing Space is treated as success """
        try:
            self.client.delete_space(AwsAccountId=self.account_id, SpaceId=space_id)
            logger.info(f'Space {space_id!r} deleted')
        except self.client.exceptions.ResourceNotFoundException:
            logger.debug(f'Space {space_id!r} was already deleted')

    def current_resources(self, space_id: str) -> dict:
        """ Resources attached to a Space: ARN -> ResourceType """
        response = self.client.list_space_resources(AwsAccountId=self.account_id, SpaceId=space_id)
        return {
            item['ResourceDetails']['resourceArn']: item.get('ResourceType')
            for item in response.get('SpaceResources') or []
            if (item.get('ResourceDetails') or {}).get('resourceArn')
        }

    def current_resource_arns(self, space_id: str) -> set:
        """ ARNs of the resources attached to a Space """
        return set(self.current_resources(space_id))

    def grant_owner(self, space_id: str, principal_arn: str) -> None:
        """ Grant the full owner action set to a principal """
        self.client.update_space_permissions(
            AwsAccountId=self.account_id,
            SpaceId=space_id,
            GrantPermissions=[{'Principal': principal_arn, 'Actions': list(SPACE_OWNER_ACTIONS)}],
        )
        logger.info(f'Granted space owner permissions on {space_id!r} to {principal_arn!r}')

    def find_by_name(self, name: str) -> list:
        """ Space ids whose display name equals ``name`` (several on a name collision) """
        space_ids = []
        parameters = {
            'AwsAccountId': self.account_id,
            'Filters': [{'name': 'SPACE_NAME', 'operator': 'STRING_EQUALS', 'value': name}],
        }
        while True:
            response = self.client.search_spaces(**parameters)
            space_ids += [summary['spaceId'] for summary in response.get('SpaceSummaries') or [] if summary.get('spaceId')]
            if not response.get('NextToken'):
                return space_ids
            parameters['NextToken'] = response['NextToken']

    def update_resources(self, space_id: str, desired_arns, resource_type: str='DASHBOARD') -> list:
        """ Add the desired resources that are not attached yet (never removes anything).

        UpdateSpaceResources does not validate that resources exist, so the caller passes
        only resources confirmed deployed. Returns the FailedResourceOperations list.
        """
        to_add = compute_space_additions(self.current_resource_arns(space_id), desired_arns)
        if not to_add:
            return []
        response = self.client.update_space_resources(
            AwsAccountId=self.account_id,
            SpaceId=space_id,
            AddResources=[{'ResourceType': resource_type, 'ResourceDetails': {'resourceArn': arn}} for arn in sorted(to_add)],
        )
        return self._report_failures(space_id, response, len(to_add), resource_type)

    def remove_resources(self, space_id: str, resources: dict) -> list:
        """ Remove resources (ARN -> ResourceType) from a Space; returns the FailedResourceOperations list """
        if not resources:
            return []
        response = self.client.update_space_resources(
            AwsAccountId=self.account_id,
            SpaceId=space_id,
            RemoveResources=[{'ResourceType': resource_type, 'ResourceDetails': {'resourceArn': arn}}
                             for arn, resource_type in sorted(resources.items())],
        )
        return self._report_failures(space_id, response, len(resources), 'removal')

    @staticmethod
    def _report_failures(space_id, response, requested, label) -> list:
        """ Warn about each failed resource operation and log the outcome """
        failed = response.get('FailedResourceOperations') or []
        for failure in failed:
            arn = (failure.get('ResourceDetails') or {}).get('resourceArn', '<unknown>')
            logger.warning(f"Failed to update {failure.get('ResourceType', label)} resource {arn!r} in space "
                           f"{space_id!r}: {failure.get('ErrorMessage')}. Continuing with the remaining resources.")
        logger.info(f'Space {space_id!r} resources updated ({label}): {requested - len(failed)} succeeded, {len(failed)} failed')
        return failed

    def write_marker(self, space_id: str, timeout: int=300, interval: int=5) -> None:
        """ Ensure the CID-managed marker is in the Space description (retried while the Space settles) """
        def _write():
            space = self.get(space_id) or {}
            description = str(space.get('description') or '')
            if CID_MANAGED_MARKER in description:
                return
            self.client.update_space(
                AwsAccountId=self.account_id,
                SpaceId=space_id,
                Name=space.get('name') or space_id,
                Description=f'{description} {CID_MANAGED_MARKER}'.strip(),
            )
        try:
            retry_on_conflict(_write, self.client.exceptions.ConflictException, timeout=timeout, interval=interval)
        except self.client.exceptions.ConflictException as exc:
            # the provenance tag is written separately, so CID-managed detection still works
            logger.warning(f'Could not write the CID-managed marker on space {space_id!r}: still busy after {timeout}s ({exc}).')
