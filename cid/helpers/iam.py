''' IAM helper
'''
import json
import copy
import time
import logging
from functools import lru_cache

import boto3
from tqdm import tqdm

from cid.exceptions import CidCritical, CidError
from cid.base import CidBase
from cid.utils import get_parameter

logger = logging.getLogger(__name__)


class IAM(CidBase):
    """ IAM helper

    Approximate list of permissions needed for creation of IAM roles and policies:
        iam:ListAttachedRolePolicies
        iam:AttachRolePolicy
        iam:DetachRolePolicy
        iam:CreateRole
        iam:GetRole
        iam:DeleteRole
        iam:CreatePolicy
        iam:DeletePolicy
        iam:GetPolicy
        iam:CreatePolicyVersion
        iam:DeletePolicyVersion
        iam:GetPolicyVersion
        iam:ListPolicyVersions
    """

    def __init__(self, session):
        super().__init__(session)
        self.client = self.session.client('iam')

    @lru_cache(1000)
    def list_attached_policies(self, role_name) -> list:
        """ List attached policies for the role
        """
        return list(self.client.get_paginator("list_attached_role_policies").paginate(RoleName=role_name).search('AttachedPolicies.PolicyArn'))

    @lru_cache(1000)
    def ensure_managed_policies_attached(self, role_name, policies_arns='') -> None:
        """ Make sure policies are attached to the role
        """
        policies_arns = [arn for arn in policies_arns.split(',') if arn]
        if not policies_arns:
            logger.debug('No need to attach roles')
            return

        if role_name in ['aws-quicksight-service-role-v0']:
            logger.info(f'{role_name} is a managed role. Please manage it in QuickSight UI. Make sure that equivalent of those polices is configured: {policies_arns}')
            return

        try:
            attached_policies = self.list_attached_policies(role_name)
        except self.client.exceptions.ClientError as exc:
            logger.info(f'Unable to list attached policies {role_name}: {exc}')
            return

        for policy_arn in policies_arns:
            if policy_arn not in attached_policies:
                try:
                    self.client.attach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy_arn,
                    )
                    logger.info(f'Attached {policy_arn} to the role {role_name}')
                except self.client.exceptions.ClientError as exc:
                    logger.info(f'Unable to attach policy {policy_arn} to {role_name}: {exc}')


    def ensure_role_for_crawler(self, s3bucket, database, table, role_name: str='CidCmdCrawlerRole', policy_name: str="CidCmdGlueCrawlerPolicy") -> None:
        """Ensure Role for Crawler exists"""

        return self.ensure_role_with_policy(
            role_name=role_name,
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            },
            permissions_policies={
                policy_name: {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowListBucket",
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": [f"arn:{self.partition}:s3:::{s3bucket}"]
                        },
                        {
                            "Sid": "AllowReadFromBucket",
                            "Effect": "Allow",
                            "Action": [ "s3:GetObject"],
                            "Resource": [f"arn:{self.partition}:s3:::{s3bucket}/*"]
                        },
                        {
                            "Sid": "AllowGlueActions",
                            "Effect": "Allow",
                            "Action": [
                                "glue:GetDatabase",
                                "glue:GetDatabases",
                                "glue:CreateTable",
                                "glue:GetTable",
                                "glue:GetTables",
                                "glue:UpdateTable",
                                "glue:GetTableVersion",
                                "glue:GetTableVersions",
                                "glue:DeleteTableVersion",
                                "glue:CreatePartition",
                                "glue:BatchCreatePartition",
                                "glue:GetPartition",
                                "glue:GetPartitions",
                                "glue:BatchGetPartition",
                                "glue:UpdatePartition",
                                "glue:DeletePartition",
                            ],
                            "Resource": [
                                f"arn:{self.partition}:glue:{self.region}:{self.account_id}:catalog",
                                f"arn:{self.partition}:glue:{self.region}:{self.account_id}:database/{database}",
                                f"arn:{self.partition}:glue:{self.region}:{self.account_id}:table/{database}/{table}",
                            ]
                        },
                        {
                            "Sid": "AllowWriteLogs",
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                                "logs:CreateLogGroup",
                            ],
                            "Resource": [f"arn:{self.partition}:logs:*:*:/aws-glue/*"]
                        },
                    ]
                }
            },
            policy_merge_mode='MERGE_RESOURCES',
            managed_policies=[]
        )
    def ensure_data_source_role_exists(self, role_name, databases, workgroup, kms_key_arns='', buckets=[], output_location_bucket=None):
        ''' Create or update a role specifically for a QS Datasource
        '''
        return self.ensure_role_with_policy(
            role_name=role_name,
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "quicksight.amazonaws.com"},
                        "Action": "sts:AssumeRole"
                    }
                ]
            },
            permissions_policies={
                'AthenaAccess': {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowAthenaReads",
                            "Effect": "Allow",
                            "Action": [
                                "lakeformation:GetDataAccess",
                                "athena:ListDataCatalogs",
                                "athena:ListDatabases",
                                "athena:ListTableMetadata",
                            ],
                            "Resource": "*",  # required https://docs.aws.amazon.com/lake-formation/latest/dg/access-control-underlying-data.html
                                              # Cannot restrict this. See https://docs.aws.amazon.com/athena/latest/ug/datacatalogs-example-policies.html#datacatalog-policy-listing-data-catalogs
                        },
                        {
                            "Sid": "AllowGlue",
                            "Effect": "Allow",
                            "Action": [
                                "glue:GetPartition",
                                "glue:GetPartitions",
                                "glue:GetDatabases",
                                "glue:GetDatabase",
                                "glue:GetTable",
                                "glue:GetTables",
                            ],
                            "Resource": [
                                f"arn:{self.partition}:glue:{self.region}:{self.account_id}:catalog",
                            ] + sum([
                                [f"arn:{self.partition}:glue:{self.region}:{self.account_id}:database/{database}",
                                f"arn:{self.partition}:glue:{self.region}:{self.account_id}:table/{database}/*" ]
                            for database in databases ], []),
                        },
                        {
                            "Sid": "AllowAthena",
                            "Effect": "Allow",
                            "Action": [
                                "athena:ListDatabases",
                                "athena:ListDataCatalogs",
                                "athena:ListDatabases",
                                "athena:GetQueryExecution",
                                "athena:GetQueryResults",
                                "athena:StartQueryExecution",
                                "athena:GetQueryResultsStream",
                                "athena:GetTableMetadata",
                            ],
                            "Resource": [
                                f"arn:{self.partition}:athena:{self.region}:{self.account_id}:datacatalog/AwsDataCatalog", # TODO: check if this can be variable?
                                f"arn:{self.partition}:athena:{self.region}:{self.account_id}:workgroup/{workgroup}",
                            ]
                        },
                        {
                            "Sid": "AllowReadAndWriteToWorkgroupOutput",
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetBucketLocation",
                                "s3:ListBucket",
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:ListBucketMultipartUploads",
                                "s3:ListMultipartUploadParts",
                                "s3:AbortMultipartUpload",
                            ],
                            "Resource": [
                                f"arn:{self.partition}:s3:::{output_location_bucket}",
                                f"arn:{self.partition}:s3:::{output_location_bucket}/*",
                            ]
                        } if output_location_bucket else None,
                        {
                            "Sid": "AllowListBucket",
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": [f"arn:{self.partition}:s3:::{bucket}" for bucket in buckets]
                        },
                        {
                            "Sid": "AllowReadBucket",
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                            "Resource": [f"arn:{self.partition}:s3:::{bucket}/*" for bucket in buckets]
                        },
                        {
                            "Sid": "NeedQuickSightDataSourceKMS",
                            "Effect": "Allow",
                            "Action": [ "kms:Decrypt"],
                            "Resource": kms_key_arns.split(','),
                        } if kms_key_arns else None,
                    ]
                }
            },
            policy_merge_mode='MERGE_RESOURCES',
        )

    def ensure_role_with_policy(self, role_name, assume_role_policy_document, permissions_policies, managed_policies=None, policy_merge_mode='MERGE_RESOURCES') -> None:
        """Ensure Role with Policy Exists

        mode: MERGE_RESOURCES|OVERRIDE
        permissions_policies:  a dict with name of attached policy and document as a value (None for managed policies)
        """
        # Create or update role
        need_a_sleep = False
        try:
            self.client.get_role(RoleName=role_name)
            logger.info(f'Role {role_name} exists')
            self.client.update_assume_role_policy(RoleName=role_name, PolicyDocument=json.dumps(assume_role_policy_document))
            logger.info(f'Role {role_name} updated')
        except self.client.exceptions.NoSuchEntityException:
            self.client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(assume_role_policy_document))
            logger.info(f'Role {role_name} created')
            need_a_sleep = True

        #Filter out all empty statements in policy
        for policy in permissions_policies.values():
            policy['Statement'] = [statement for statement in policy['Statement'] if statement]

        # Create or update policies and attach them to the role
        for policy_name, new_policy_document in permissions_policies.items():
            try:
                policy_arn = f"arn:{self.partition}:iam::{self.account_id}:policy/{policy_name}"
                policy_response = self.client.get_policy(PolicyArn=policy_arn)
                logger.info(f"Policy '{policy_name}' already exists.")

                # If the policy exists, it probably requires an update
                if isinstance(new_policy_document, dict):
                    policy_versions = self.client.list_policy_versions(PolicyArn=policy_arn)['Versions']
                    if len(policy_versions) >= 5: #policy cannot contain more than 5 versions, so we need to delete
                        for version in policy_versions[4:]:
                            self.client.delete_policy_version(PolicyArn=policy_arn, VersionId=version['VersionId'])
                            logger.info(f"Deleted policy version {version['VersionId']}.")
                    version = policy_response['Policy']['DefaultVersionId']
                    current_policy_document = self.client.get_policy_version(PolicyArn=policy_arn, VersionId=version)['PolicyVersion']['Document']

                    if policy_merge_mode == "MERGE_RESOURCES":
                        result_document = merge_policy_docs_on_resource_level(current_policy_document, new_policy_document)
                    else:
                        result_document = new_policy_document

                    logger.debug('current_policy_document=' + json.dumps(current_policy_document))
                    logger.debug('new_policy_document=' + json.dumps(new_policy_document))
                    logger.debug('result_document=' + json.dumps(result_document))
                    if result_document == current_policy_document:
                        logger.info(f"No need to update '{policy_name}' the doc is the same as requested.")
                    else:
                        logger.debug(f'Updating policy {policy_arn} to {result_document}')
                        print(f'Updating policy {policy_arn} to :\n{json.dumps(result_document, indent=2)}')
                        if get_parameter(f'confirm-policy-{policy_name}', message='Please confirm', choices=['yes', 'no']) != 'yes':
                            raise CidCritical('User choose not to confirm challenge')

                        # A managed policy can have up to 5 versions.
                        # Before you create a new version, you must delete at least one existing version.
                        versions = self.client.list_policy_versions(PolicyArn=policy_arn)['Versions']
                        if len(versions) >= 5:
                            for version in versions[::-1]:
                                if not version['IsDefaultVersion']:
                                    continue
                                self.client.delete_policy_version(PolicyArn=policy_arn, VersionId=version['VersionId'])
                                break
                        self.client.create_policy_version(PolicyArn=policy_arn, PolicyDocument=json.dumps(result_document), SetAsDefault=True)
                        logger.info(f"Policy '{policy_name}' updated.")
            except self.client.exceptions.NoSuchEntityException:
                policy_response = self.client.create_policy(
                    PolicyName=policy_name,
                    PolicyDocument=json.dumps(new_policy_document)
                )
                logger.info(f"Policy '{policy_name}' created successfully.")

            # Attach if needed
            if not any(arn == policy_arn for arn in self.iterate_policies(role_name)):
                self.client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                logger.info(f"IAM Policy '{policy_name}' attached to the role.")
                need_a_sleep = True


        for managed_policy_arn in managed_policies or []:
            if not any(arn == managed_policy_arn for arn in self.iterate_policies(role_name)):
                self.client.attach_role_policy(RoleName=role_name, PolicyArn=managed_policy_arn)
                logger.info(f"Managed Policy '{managed_policy_arn}' attached to the role.")

        if need_a_sleep:
            # Some times the role cannot be assumed without this delay after creation
            for _ in tqdm(range(10), desc=f'Waiting for Role {role_name}', leave=False):
                time.sleep(1)

        return role_name

    def iterate_policies(self, role_name, search='AttachedPolicies[].PolicyArn'):
        yield from self.client.get_paginator('list_attached_role_policies').paginate(RoleName=role_name).search(search)

    def iterate_role_names(self, search='Roles[].RoleName'):
        """ Iterate role names
        """
        try:
            yield from self.client.get_paginator('list_roles').paginate().search(search)
        except self.client.exceptions.ClientError as exc:
            logger.warning('Failed to read available IEM roles: {exc}. Most likely not fatal.')

    def get_role_arn(self, role_name:str) -> str:
        """ Get role arn, and try the best guess if no permissions
        """
        try:
            return self.client.get_role(RoleName=role_name)['Role']['Arn']
        except self.client.exceptions.NoSuchEntityException:
            raise CidError(f"Role '{role_name}' does not exist.")
        except self.client.exceptions.ClientError as exc:
            if "AccessDenied" in str(exc):
                logger.debug('Got access denied for describing role. Try the best guess')
                if role_name.startswith('aws-quicksight-service-role'):
                    return f'arn:aws:iam::{self.account_id}:role/service-role/{role_name}'
                else:
                    return f'arn:aws:iam::{self.account_id}:role/{role_name}'
            raise CidError(f"An error occurred: {exc}")

    def ensure_role_does_not_exist(self, role_name):
        """ Remove a role and all policies if they are not used in other roles
        """
        try:
            self.client.get_role(RoleName=role_name)
        except self.client.exceptions.NoSuchEntityException:
            logger.debug(f'No role {role_name}')
            return

        # Clean all attached policies
        for arn in self.client.get_paginator('list_attached_role_policies').paginate(RoleName=role_name).search('AttachedPolicies[].PolicyArn'):
            logger.debug(f'Detaching {arn} from {role_name}')
            self.client.detach_role_policy(PolicyArn=arn, RoleName=role_name)
            if 'aws:policy/service-role/' in arn:
                continue # skip managed policies
            for version in self.client.list_policy_versions(PolicyArn=arn)['Versions']:
                if not version['IsDefaultVersion']:
                    self.client.delete_policy_version(PolicyArn=arn, VersionId=version['VersionId'])
            logger.debug(f'Deleting {arn}')
            try:
                self.client.delete_policy(PolicyArn=arn)
            except self.client.exceptions.DeleteConflictException:
                logger.debug(f'Policy {arn} is still used. Skipping')

        # Delete role
        logger.debug(f'Deleting role {role_name!r}')
        try:
            self.client.delete_role(RoleName=role_name)
        except self.client.exceptions.DeleteConflictException:
            logger.debug(f'Role {role_name!r} is still used. Skipping')




def merge_policy_docs_on_resource_level(old_doc, new_doc):
    """ Returns a merged doc with statement from 2 IAM policy documents.

    This will not delete any statement, just add combining resources in statements with the same Sid.
    Resources without Sid will be just added.
    """
    res_doc = copy.deepcopy(old_doc)
    for new_statement in new_doc['Statement']:
        for res_statement in res_doc['Statement']:
            if new_statement.get('Sid') and new_statement.get('Sid') == res_statement.get('Sid'):
                assert res_statement['Effect'] == new_statement['Effect'], f"Different Effects are not supported {res_statement} vs {new_statement}."
                assert sorted(res_statement['Action']) == sorted(new_statement['Action']), f"Different Actions are not supported {res_statement} vs {new_statement}."
                res_statement_res = res_statement['Resource']
                new_statement_res = new_statement['Resource']
                if isinstance(res_statement_res, str):
                    res_statement_res = [res_statement_res]
                if isinstance(new_statement_res, str):
                    new_statement_res = [new_statement_res]
                merged_res = sorted(list(set(res_statement_res + new_statement_res)))
                if len(merged_res) == 1: merged_res = merged_res[0]
                res_statement['Resource'] = merged_res
                break
        else:
            res_doc['Statement'].append(new_statement)
    return res_doc

def test_merge_policy_docs_on_resource_level():
    doc1 = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Sid1",
                "Effect": "Allow",
                "Action": "s3:ListAllMyBuckets",
                "Resource": "arn:aws:s3:::*"
            },
            {
                "Sid": "Sid2",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": [
                    "arn:aws:s3:::bucket_name1",
                    "arn:aws:s3:::bucket_name2"
                ]
            }
        ]
    }
    doc2 = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Sid1",
                "Effect": "Allow",
                "Action": "s3:ListAllMyBuckets",
                "Resource": "arn:aws:s3:::*"
            },
            {
                "Sid": "Sid2",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": [
                    "arn:aws:s3:::bucket_name3"
                ]
            },
            {
                "Sid": "Sid3",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": [
                    "arn:aws:s3:::bucket_name4"
                ]
            }
        ]
    }
    res = merge_policy_docs_on_resource_level(doc1, doc2)
    assert 'arn:aws:s3:::bucket_name1' in res['Statement'][1]['Resource'], "Merge must contain resources"
    assert 'arn:aws:s3:::bucket_name2' in res['Statement'][1]['Resource'], "Merge must contain resources"
    assert 'arn:aws:s3:::bucket_name3' in res['Statement'][1]['Resource'], "Merge must contain resources"
    assert "arn:aws:s3:::*" == res['Statement'][0]['Resource'], "Merge same resources must be identical to the source"
    assert ["arn:aws:s3:::bucket_name4"] == res['Statement'][2]['Resource'], "New items must be added"


def integration_test_create_delete_role():
    """ A quicks test for creation and deletion
    """
    iam = IAM(boto3.session.Session())
    role_name = iam.ensure_data_source_role_exists('test_role', buckets=['test'])
    assert role_name
    attached_policies = { pol['PolicyName']: pol['PolicyArn'] for pol in iam.iterate_policies(role_name)}
    assert 'AWSQuicksightAthenaAccess' in attached_policies

    iam.ensure_role_does_not_exist(role_name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('cid').setLevel(logging.DEBUG)
    integration_test_create_delete_role()
