""" Account Map

Account Map is an athena view that typically contains 'account_id' 'account_name' and other attributes of account (Business Unit, Scope, Cost Center or other).

There are several Inputs and several Outputs that we need to support.

# Output
There are 2 kinds of account map supported as output:
 1) account_map (used in most dashboards)
    Columns:
        account_id,
        account_name,
        plus additional fields can be added by user

 2) aws_accounts (used only in Trends)
    Columns:
        account_id,
        account_name,
        parent_account_id,
        account_email_id,
        account_status

# Input
this module supports several input
1) Pull from account metadata table (table containing accounts and data like name, tags, orgs etc). Several tables can be used to source information about Accounts. One is the source from organization_data provided by CID data collection lab
2) Dummy - a simple mapping from CUR.
3) CSV file in a format compatible with AWS Organization CSV output #FIXME add link
4) AWS Organization (rare and not recommended case when we install in Management Account or Delegated account that has access to the AWS Org)

If user do not specify the choice explicitly, we will try to autodetect. If metadata table is found we will use that if not - we will propose a choice to the user. If unattended mode will use dummy.

"""

import os
import logging
from pathlib import Path
from functools import lru_cache

from string import Template
from boto3.session import Session
from importlib import resources

from cid.base import CidBase
from cid.helpers import Athena, CUR
from cid.utils import get_parameter, get_parameters, cid_print, unset_parameter, set_parameters
from cid.exceptions import CidCritical, CidError
from cid.helpers.csv2view import read_csv

logger = logging.getLogger(__name__)

class AccountMap(CidBase):
    """ Create an account_map view or aws_accounts
    """
    # different metadata tables have different keys for the information we need. So here is a mapping showing for each
    # account map we support, what are the keys depending on the source table:
    # mappings[target_map][source][key] = key_in_source table
    mappings = {
        'account_map': {
            'acc_metadata_details': {'account_id': 'account_id', 'account_name': 'account_name'},
            'organization_data': {'account_id': 'id', 'account_name': 'name', 'email': 'email'}
        },
        'aws_accounts': {
            'acc_metadata_details': {'account_id': 'account_id', 'account_name': 'account_name', 'email': 'email'},
            'organization_data': { 'account_id': 'id', 'account_name': 'name', 'email': 'email', 'status': 'status'},
        }
    }

    def __init__(self, session: Session, athena: Athena, get_cur=None) -> None:
        self.get_cur = get_cur # get_cur callback Only for trends and dummy
        self.athena = athena
        super().__init__(session)

    @lru_cache(1000)
    def detect_metadata_table(self, target_name):
        """ Detect a table with the list of accounts

        This function scans all tables in the current database and few others, searching for a table
        with one of given names and a set of field. The found table most likely contains the list of
        accounts and their metadata.

        :target view: a target view (output)
        :returns: a table object
        """
        cid_print('Autodiscover metadata table')
        databases_to_look_for = set([
            self.athena.DatabaseName, # current database
            'organization_data', #  from data collection
            'organisation_data', #  from older versions of data collection
            # FIXME probably also can be customizable via command line parameter
        ])
        found_metadata_table = None
        for database_name in databases_to_look_for:
            try:
                athena = Athena(session=self.athena.session, database_name=database_name)
                tables = athena.list_table_metadata()
            except Exception as exc:
                logger.debug(f'{type(exc)}: {exc}')
                continue
            tables = [t for t in tables if t.get('TableType') == 'EXTERNAL_TABLE'] #filter only tables
            tables = [t for t in tables if t.get('Name') in self.mappings[target_name]] #filter only with supported names
            for table in tables:
                table_name = table.get('Name')
                logger.debug(f"Detected table {table_name}")
                field_found = [col.get('Name') for col in table.get('Columns')]
                field_required = list(self.mappings[target_name][table_name].values())
                # Check if we have all the required fields
                if all(field in field_found for field in field_required):
                    logger.debug(f'All required fields found in {database_name}.{table_name} ')
                    table['Database'] = database_name
                    found_metadata_table = table # return the first found. FIXME: probably need a choice if more then one
                    break
            if found_metadata_table:
                break
        return found_metadata_table

    def create_sql_from_metadata(self, name, metadata_table):
        ''' create account map from the found metadata table '''
        view_definition = self.athena._resources.get('views').get(name, {})
        if view_definition.get('File'):
            view_file = view_definition.get('File')
            template = Template(
                (resources.files(view_definition.get('providedBy')) / f'data/{view_file}').read_text()
            )
        elif view_definition.get('data'):
            template = Template(str(view_definition.get('data')))
        else:
            raise CidError(f'{name} definition does not contain File or data: {view_definition}')

        # Fill template variables
        vars = {
            'metadata_table_name': metadata_table['Name'],
            'metadata_database_name': metadata_table['Database'],
            'cur_database': self.get_cur().database if name == 'aws_accounts' and self.get_cur else None, # only for trends in metadata
            'cur_table_name': self.get_cur().table_name if name == 'aws_accounts' and self.get_cur else None, # only for trends in metadata
        }
        for key, val in self.mappings.get(name).get(metadata_table['Name']).items():
            logger.debug(f'Mapping field {key} to {val}')
            vars[key] = val
        compiled_query = template.safe_substitute(vars)
        return compiled_query

    def create_or_update(self, name) -> bool:
        """Create account map"""
        cid_print(f'Creating account mapping <BOLD>{name}<END>')
        compiled_query = self.create_account_mapping_sql(name)
        self.athena.create_or_update_view(name, compiled_query)

    def get_dummy_account_mapping_sql(self, name) -> list:
        """Create dummy account mapping"""
        logger.info(f'Creating dummy account mapping for {name}')
        cur = self.get_cur()
        if cur.version.startswith('2'):
            sql_file = 'data/queries/shared/account_map_cur2.sql'
        else:
            sql_file = 'data/queries/shared/account_map_dummy.sql'
        template = Template(
            (resources.files('cid.builtin.core') / sql_file).read_text()
        )
        columns_tpl = {
            'athena_view_name': name,
            'cur_table_name': cur.table_name,
            'cur_database': cur.database,
        }
        compiled_query = template.safe_substitute(columns_tpl)
        return compiled_query

    def get_organization_accounts(self) -> list:
        """ Retrieve AWS Organization account """
        cid_print('\nCollecting account info from AWS Organizations')
        accounts = []
        orgs = self.session.client('organizations')
        try:
            for account in orgs.get_paginator('list_accounts').paginate().search('Accounts'):
                accounts.append({
                    'account_id': account.get('Id'),
                    'account_name': account.get('Name'),
                    'account_status': account.get('Status'), #Only for Trends
                    'account_email': account.get('Email'), #Only for Trends
                })
            cid_print(f'{len(accounts)} accounts collected')
        except orgs.exceptions.AWSOrganizationsNotInUseException:
            cid_print('AWS Organization is not enabled')
        except orgs.exceptions.AccessDeniedException:
            cid_print('No access to AWS Organization.')
        except Exception as exc: #pylint: disable=broad-exception-caught
            cid_print(exc)

        return accounts

    def check_file_exists(self, file_path) -> bool:
        """ Checks if the given file exists """
        # Set base paths
        abs_path = Path().absolute()
        return Path.is_file(abs_path / file_path)

    def get_csv_accounts(self) -> list:
        """ Retrieve accounts from CSV file """
        # get csv file from user
        finished = False
        while not finished:
            mapping_file = get_parameter(
                param_name='account-map-file',
                message="Enter file path",
            )
            mapping_file = os.path.expanduser(mapping_file)
            finished = self.check_file_exists(mapping_file)
            if not finished:
                cid_print(f'File not found: {mapping_file}')
                unset_parameter('account-map-file')
        cid_print(f'\nReading {mapping_file}')

        accounts = [
            {str(k).lower().replace(" ", "_"): str(v) for k, v in row.items()}
            for row in read_csv(mapping_file)
        ]
        cid_print(f'{len(accounts)} accounts collected')
        return accounts


    def get_sql_for_accounts(self, name, accounts) -> str:
        ''' return a sql when given accounts'''

        # Different tables have different keys, so we need to align keys from different source tables
        accounts = accounts.copy()
        # Required keys mapping, used in renaming below
        key_mapping = {
            'accountid': 'account_id',
            'name': 'account_name'
        }
        for account in accounts:
            # Rename required keys according to expected names
            for old_key, new_key in key_mapping.items():
                if not account.get(new_key) and account.get(old_key):
                    account[new_key] = account[old_key]
                    del account[old_key]
            # Fill optional keys with defaults
            account.update({
                'parent_account_id': account.get('parent_account_id', '0'),
                'account_status': account.get('account_status', 'unknown'),
                'account_email': account.get('account_email', 'unknown')
            })

        template_str = '''CREATE OR REPLACE VIEW  ${athena_view_name} AS
            SELECT
                *
            FROM
                ( VALUES ${rows} )
            ignored_table_name (account_id, account_name, parent_account_id, account_status, account_email)
        '''
        template = Template(template_str)
        accounts_sql = []
        row_template = """ROW ('{account_id}', '{account_name}:{account_id}', '{parent_account_id}', '{account_status}', '{account_email}')"""
        for account in accounts:
            acc = account.copy()
            acc['account_name'] = acc['account_name'].replace("'", "''") #escape SQL characters
            accounts_sql.append(row_template.format(**acc))
        # Fill template
        columns_tpl = {
            'athena_view_name': name,
            'rows': ','.join(accounts_sql)
        }
        compiled_query = template.safe_substitute(columns_tpl)
        return compiled_query


    def create_account_mapping_sql(self, name) -> str:
        """ Returns Athena query for account mapping, based on user's choice and reasonable defaults """

        if not get_parameters().get('account-map-source'):
            # try to find a table with data about accounts if no explicit choice
            try:
                metadata_table = self.detect_metadata_table(name)
                if metadata_table:
                    return self.create_sql_from_metadata(name=name, metadata_table=metadata_table)
            except CidError as exc:
                logger.debug(exc)

        if self.get_cur and self.get_cur().version.startswith('2'):
            cid_print('Looks like CUR2 is used. Will use it for account map.')
            set_parameters({'account-map-source': 'dummy'})

        for attempt in range(3):
            try:
                choices = {
                    'AWS Organizations (one time account listing)': 'organization',
                    'CSV file (relative path required)': 'csv',
                }
                if self.get_cur:
                    choices['Dummy (CUR account data, no names for CUR1)'] = 'dummy'

                metadata_source = get_parameter(
                    param_name='account-map-source',
                    message="Please select account metadata collection method",
                    choices=choices,
                )
                logger.info(f'Attempt {attempt + 2}' )
                # Collect account list from different sources of user choice
                if metadata_source == 'csv':
                    accounts = self.get_csv_accounts()
                    compiled_query = self.get_sql_for_accounts(name=name, accounts=accounts)
                elif metadata_source == 'organization':
                    accounts = self.get_organization_accounts()
                    compiled_query = self.get_sql_for_accounts(name=name, accounts=accounts)
                elif metadata_source == 'dummy':
                    compiled_query = self.get_dummy_account_mapping_sql(name)
                else:
                    cid_print('Unsupported selection')
                return compiled_query
            except Exception as exc:
                logger.debug(exc)
        else:
            raise CidCritical('Failed to create account map')