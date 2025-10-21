import re
import json
import time
import logging

from cid.base import CidBase
from cid.helpers import S3
from cid.utils import get_parameter, get_parameters, cid_print, isatty, unset_parameter, get_yesno_parameter
from cid.helpers.diff import diff
from cid.exceptions import CidCritical, CidError

logger = logging.getLogger(__name__)

class Athena(CidBase):
    # Define defaults
    defaults = {
        'CatalogName': 'AwsDataCatalog',
        'DatabaseName': 'customer_cur_data',
        'WorkGroup': 'CID'
    }
    _CatalogName = None
    _DatabaseName = None
    _WorkGroup = None
    _metadata = dict()
    _resources = dict()
    _client = None

    def __init__(self, session, resources: dict=None, database_name: str=None) -> None:
        super().__init__(session)
        self._resources = resources
        if database_name:
           self.DatabaseName = database_name # this can raise AccessDenied or CidError if database not found

    @property
    def client(self):
        if not self._client:
            self._client = self.session.client('athena', region_name=self.region)
        return self._client

    @property
    def CatalogName(self) -> str:
        """ Check if AWS DataCatalog and Athena database exist """
        if not self._CatalogName:
            glue_data_catalogs = self.list_data_catalogs()
            if not glue_data_catalogs:
                raise CidCritical('AWS DataCatalog of type GLUE not found!')
            if len(glue_data_catalogs) == 1:
                self._CatalogName = glue_data_catalogs[0]
            elif len(glue_data_catalogs) > 1:
                # Ask user
                self._CatalogName = get_parameter(
                    param_name='glue-data-catalog',
                    message="Select AWS DataCatalog to use",
                    choices=glue_data_catalogs,
                )
            logger.info(f'Using DataCatalog: {self._CatalogName}')
        return self._CatalogName

    @CatalogName.setter
    def set_catalog_name(self, catalog):
        self._CatalogName = catalog

    @property
    def DatabaseName(self) -> str:
        """ Check if Athena database exist """
        if self._DatabaseName:
            return self._DatabaseName

        if get_parameters().get('athena-database'):
            self._DatabaseName = get_parameters().get('athena-database')
            try:
                if not self.get_database(self._DatabaseName):
                    raise CidCritical(f'Database {self._DatabaseName} not found in Athena catalog {self.CatalogName}')
            except Exception as exc:
                if 'AccessDeniedException' in str(exc):
                    logger.warning(f'{type(exc)} - Missing athena:GetDatabase permission. Cannot verify existence of {self._DatabaseName} in {self.CatalogName}. Hope you have it there.')
                else:
                    raise
            return self._DatabaseName
        # Get AWS Athena databases
        athena_databases = self.list_databases()

        # check if we have a default database
        logger.info(f'athena_databases = {athena_databases}')
        default_databases = [database for database in athena_databases if database == self.defaults.get('DatabaseName')]
        if 'cid_cur' in athena_databases:
            default_databases = ['cid_cur']

        # Ask user
        choices = list(athena_databases)
        if self.defaults.get('DatabaseName') not in choices:
            choices.append(self.defaults.get('DatabaseName') + ' (CREATE NEW)')
        self._DatabaseName = get_parameter(
            param_name='athena-database',
            message="Select AWS Athena database to use as default",
            choices=choices,
            default=default_databases[0] if default_databases else None,
        )
        if self._DatabaseName.endswith( ' (CREATE NEW)'):
            self._DatabaseName = self.defaults.get('DatabaseName')
            self.query(f'CREATE DATABASE {self._DatabaseName}')
        logger.info(f'Using Athena database: {self._DatabaseName}')
        return self._DatabaseName

    @DatabaseName.setter
    def DatabaseName(self, database_name):
        database = self.get_database(database_name) # this can raise AccessDenied error
        if database:
            self._DatabaseName = database_name
        else:
            raise CidError(f'Database {database_name} not found')

    @property
    def WorkGroup(self) -> str:
        """ Select AWS Athena workgroup """
        if not self._WorkGroup:
            if get_parameters().get('athena-workgroup'):
                self.WorkGroup = self._ensure_workgroup(name=get_parameters().get('athena-workgroup'))
                return self._WorkGroup
            logger.info('Selecting Athena workgroup...')
            workgroups = self.list_work_groups()
            logger.info(f'Found {len(workgroups)} workgroups: {", ".join([wg.get("Name") for wg in workgroups])}')
            # if len(workgroups) == 0:
            #     self.WorkGroup = self._ensure_workgroup(name=self.defaults.get('WorkGroup'))
            # elif len(workgroups) == 1:
            #     # Silently choose the only workgroup that is available
            #     self.WorkGroup = self._ensure_workgroup(name=workgroups.pop().get('Name'))
            # else:
            # Select default workgroup if present
            if self.defaults.get('WorkGroup') not in {wgr['Name'] for wgr in workgroups}:
                workgroups.append({'Name': f"{self.defaults.get('WorkGroup')} (create new)"})
            default_workgroup = next(iter([wgr.get('Name') for wgr in workgroups if  self.defaults.get('WorkGroup') in wgr['Name']]), None)
            if default_workgroup:
                logger.info(f'Found "{default_workgroup}" as a default workgroup')
            # Ask user
            selected_workgroup = get_parameter(
                param_name='athena-workgroup',
                message="Select Amazon Athena workgroup to use",
                choices=[wgr['Name'] for wgr in workgroups],
                default=default_workgroup,
                fuzzy=False,
            )
            if ' (create new)' in selected_workgroup:
                selected_workgroup = selected_workgroup.replace(' (create new)', '')
            self.WorkGroup = self._ensure_workgroup(name=selected_workgroup)

            logger.info(f'Selected workgroup: "{self._WorkGroup}"')
        return self._WorkGroup

    @WorkGroup.setter
    def WorkGroup(self, name: str):
        try:
            _workgroup = self.client.get_work_group(WorkGroup=name).get('WorkGroup')
        except self.client.exceptions.InvalidRequestException as e:
            raise CidCritical(e)
        if _workgroup.get('State') == 'DISABLED':
            raise CidCritical(f'Athena Workgroup "{name}" is disabled.')
        if not _workgroup.get('Configuration', {}).get('ResultConfiguration', {}).get('OutputLocation'):
            raise CidCritical(f'Athena Workgroup "{name}" must have an output location s3 bucket configured in the region {self.region}. See https://{self.region}.console.aws.amazon.com/athena/home?#/workgroups .')
        self._WorkGroup = name
        logger.info(f'Selected Athena WorkGroup: "{self._WorkGroup}"')

    def workgroup_output_location(self) -> str:
        workgroup = self.client.get_work_group(WorkGroup=self.WorkGroup)
        return workgroup.get('WorkGroup', {}).get('Configuration', {}).get('ResultConfiguration', {}).get('OutputLocation', None)


    def _ensure_workgroup(self, name: str) -> str:
        """Ensure a workgroup exists and configured with an S3 bucket"""
        s3 = S3(session=self.session)
        if name == 'primary': # QuickSight manages primary wg differently, relying exclusively on bucket with a predefined name
            bucket_name = f'{self.partition}-athena-query-results-{self.region}-{self.account_id}'
        else:
            bucket_name = f'{self.partition}-athena-query-results-cidcmd-{self.account_id}-{self.region}'

        try:
            workgroup = self.client.get_work_group(WorkGroup=name)
            if workgroup.get('WorkGroup', {}).get('Configuration', {}).get('ResultConfiguration', {}).get('OutputLocation', None):
                return name # all good we have Output Bucket Configured.

            # there no result bucket configured for this WG
            buckets = s3.list_buckets(region_name=self.region)
            if bucket_name not in buckets:
                buckets.append(f'{bucket_name} (create new)')
            bucket_name = get_parameter(
                param_name='athena-result-bucket',
                message=f"Select S3 bucket to use with Amazon Athena Workgroup [{name}]",
                choices=[bucket for bucket in buckets]
            )
            if ' (create new)' in bucket_name:
                bucket_name = bucket_name.replace(' (create new)', '')
            s3.ensure_bucket(name=bucket_name)
            self.client.update_work_group(
                WorkGroup=name,
                Description='string',
                ConfigurationUpdates={
                    'ResultConfigurationUpdates': {
                        'OutputLocation': f's3://{bucket_name}',
                        'EncryptionConfiguration': {
                            'EncryptionOption': 'SSE_S3',
                        },
                        'AclConfiguration': {
                            'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                        }
                    }
                }
            )
            return name
        except self.client.exceptions.InvalidRequestException as exc:
            # Workgroup does not exist
            if 'WorkGroup is not found' in exc.response.get('Error', {}).get('Message'):
                s3.ensure_bucket(name=bucket_name)
                self.client.create_work_group(
                    Name=name,
                    Configuration={
                        'ResultConfiguration': {
                            'OutputLocation': f's3://{bucket_name}',
                            'EncryptionConfiguration': {
                                'EncryptionOption': 'SSE_S3',
                            },
                            'AclConfiguration': {
                                'S3AclOption': 'BUCKET_OWNER_FULL_CONTROL'
                            }
                        },
                    }
                )
                return name
            else:
                raise
        except Exception as exc:
            logger.exception(exc)
            raise CidCritical(f'Failed to create Athena work group ({exc})') from exc

    def list_data_catalogs(self, type_: str='GLUE', workgroup: str=None) -> list:
        """get data catalogs"""
        params = {}
        if workgroup:
            params['WorkGroup'] = workgroup
        return list(self.client.get_paginator('list_data_catalogs').paginate(**params).search(f"DataCatalogsSummary[?Type == '{type_}'].CatalogName"))

    def list_databases(self, catalog_name: str=None) -> list:
        """get database"""
        return list(self.client.get_paginator('list_databases').paginate(CatalogName=catalog_name or self.CatalogName).search('DatabaseList[].Name'))

    def get_database(self, DatabaseName: str=None) -> bool:
        """ Check if AWS DataCatalog and Athena database exist """
        DatabaseName = DatabaseName or self.DatabaseName
        try:
            return self.client.get_database(CatalogName=self.CatalogName, DatabaseName=DatabaseName).get('Database')
        except self.client.exceptions.ClientError as exc:
            if 'AccessDeniedException' in str(exc):
                raise
            else:
                logger.debug(exc, exc_info=True)
                return None

    def list_table_metadata(self, database_name: str=None, max_items: int=None) -> dict:
        params = {
            'CatalogName': self.CatalogName,
            'DatabaseName': database_name or self.DatabaseName,
            'PaginationConfig':{
                'MaxItems': max_items,
            },
        }
        table_metadata = list()
        try:
            paginator = self.client.get_paginator('list_table_metadata')
            response_iterator = paginator.paginate(**params)
            for page in response_iterator:
                table_metadata.extend(page.get('TableMetadataList'))
            logger.debug(f'Table metadata: {table_metadata}')
            logger.info(f'Found {len(table_metadata)} tables in {database_name or self.DatabaseName}')
        except Exception as e:
            logger.error(f'Failed to list tables in {database_name or self.DatabaseName}')
            logger.error(e)

        return table_metadata

    def list_work_groups(self) -> list:
        """ List AWS Athena workgroups """
        result = self.client.list_work_groups()
        logger.debug(f'WorkGroups: {result.get("WorkGroups")}')
        return result.get('WorkGroups')

    def get_table_metadata(self, table_name: str, database_name: str=None, no_cache: bool=False) -> dict:
        table_metadata = None
        if not no_cache:
            table_metadata = self._metadata.get(table_name)
        if not table_metadata:
            params = {
                'CatalogName': self.CatalogName,
                'DatabaseName': database_name or self.DatabaseName,
                'TableName': table_name,
            }
            table_metadata = self.client.get_table_metadata(**params).get('TableMetadata')
            self._metadata[table_name] = table_metadata

        return table_metadata


    def execute_query(self, sql_query, sleep_duration=1, database: str=None, catalog: str=None, fail: bool=True) -> str:
        """ Executes an AWS Athena Query """

        # Set execution context
        execution_context = {
            'Database': database or self.DatabaseName,
            'Catalog': catalog or self.CatalogName,
        }

        try:
            # Start Athena query
            response = self.client.start_query_execution(
                QueryString=sql_query,
                QueryExecutionContext=execution_context,
                WorkGroup=self.WorkGroup
            )

            # Get Query ID
            query_id = response.get('QueryExecutionId')
            if not query_id:
                logger.debug(f'Full query: {sql_query}')
                raise CidCritical(f'Athena cannot start query. Answer is: {response}')
            # Get Query Status for the first time
            query_status = self.client.get_query_execution(QueryExecutionId=query_id)
        except self.client.exceptions.InvalidRequestException as exc:
            logger.debug(f'Full query: {sql_query}')
            raise CidCritical(f'InvalidRequestException: {exc}') from exc
        except Exception as exc:
            logger.debug(f'Full query: {sql_query}')
            raise CidCritical(f'Query:\n{sql_query}\n\nAthena query failed: {exc}') from exc

        current_status = query_status['QueryExecution']['Status']['State']

        # Poll for the current status of query as long as its not finished
        while current_status in ['SUBMITTED', 'RUNNING', 'QUEUED']:
            response = self.client.get_query_execution(QueryExecutionId=query_id)
            current_status = response['QueryExecution']['Status']['State']

            # Sleep before polling again
            time.sleep(sleep_duration)

        if current_status == "SUCCEEDED":
            return query_id
        failure_reason = response.get('QueryExecution', {}).get('Status', {}).get('StateChangeReason',repr(response))
        logger.info(f'Athena query failed: {failure_reason}')
        logger.debug(f'Full query: {sql_query}')
        if fail:
            raise CidCritical(f'Query:\n{sql_query}\n\nAthena query status failed : {failure_reason}')
        return False

    def get_query_results(self, query_id):
        """ Get Query Results """
        paginator = self.client.get_paginator("get_query_results")
        return paginator.paginate(QueryExecutionId=query_id).build_full_result()

    def parse_response_as_table(self, response, include_header=False):
        """ Return a query response as a table. """
        result = []
        starting_row = 0 if include_header else 1
        for row in response['ResultSet']['Rows'][starting_row:]:
            result.append([data.get('VarCharValue', '') for data in row['Data']])
        return result

    def query(self, sql, include_header=False, **kwargs) -> list:
        """ Execute Athena Query and return a result"""
        logger.debug(f'query={sql}')
        execution_id = self.execute_query(sql, **kwargs)
        results = self.get_query_results(execution_id)
        #logger.debug(f'results = {json.dumps(results, indent=2)}')
        parsed = self.parse_response_as_table(results, include_header)
        logger.debug(f'parsed res = {json.dumps(parsed, indent=2)}')
        return parsed


    def discover_views(self, views: dict={}) -> None:
        """ Discover views from a given list of view names and cache them. """
        for view_name in views:
            try:
                self.get_table_metadata(table_name=view_name)
            except self.client.exceptions.MetadataException:
                pass


    def wait_for_view(self, view_name: str, poll_interval=1, timeout=60) -> None:
        """ Wait for a View to be created. """
        deadline = time.time() + timeout
        while time.time() <= deadline:
            self.discover_views([view_name])
            if view_name in self._metadata.keys():
                logger.info(f'view {view_name} exists')
                return True
            else:
                time.sleep(poll_interval)
        else:
            logger.info(f'View {view_name} does not exist, or not discoverable')
            return False


    def delete_table(self, name: str, catalog: str=None, database: str=None):
        if not get_yesno_parameter(
                param_name=f'confirm-{name}',
                message=f'Delete Athena table {name}?',
                default='no'):
            return False

        try:
            self.execute_query(
                f'DROP TABLE IF EXISTS {name};',
                catalog=catalog,
                database=database,
                fail=False
            )
        except Exception as exc:
            logger.debug(exc, exc_info=True)
            logger.info(f'Table {name} cannot be deleted: {exc}')
            return False
        else:
            if name in self._metadata: del self._metadata[name]
            logger.info(f'Table {name} deleted')
        return True

    def delete_view(self, name: str, catalog: str=None, database: str=None):
        if not get_yesno_parameter(
                param_name=f'confirm-{name}',
                message=f'Delete Athena view {name}?',
                default='no'):
            return False

        try:
            self.execute_query(
                f'DROP VIEW IF EXISTS {name};',
                catalog=catalog,
                database=database,
                fail=False
            )
        except Exception as exc:
            logger.debug(exc, exc_info=True)
            logger.info(f'View {name} cannot be deleted: {exc}')
            return False
        else:
            if name in self._metadata: del self._metadata[name]
            logger.info(f'View {name} deleted')
        return True

    def get_view_diff(self, name, sql):
        """ returns a diff between existing and new views. """
        tmp_name = 'cid_tmp_deleteme'
        existing_sql = ''
        try:
            existing_sql = self.query(f'SHOW CREATE VIEW {name}', include_header=True)
            existing_sql = '\n'.join([line[0] for line in existing_sql][1:])
        except Exception as exc:
            print(exc)
            return None
        try:
            # Avoid difference in the first line of diff (replace name of the view with the tmp_name)
            tmp_sql = re.sub(r'(CREATE OR REPLACE VIEW) (.+?) (AS.*)', r'\1 ' + tmp_name +  r' \3', sql)

            if tmp_sql == sql:
                raise CidError(f"Cannot get diff: same sql {repr(sql)}")
            self.query(tmp_sql)
            tmp_sql = self.query(f'SHOW CREATE VIEW {tmp_name}', include_header=True)
            tmp_sql = '\n'.join([line[0] for line in tmp_sql][1:])
        except Exception as exc:
            print(exc)
            return None
        finally:
            try:
                self.query(f'DROP VIEW IF EXISTS {tmp_name};', fail=False)
            except Exception as exc:
                logger.debug(f'Cannot delete temporary view {tmp_name}: {exc}')

        existing_sql = re.sub('"(.+?)"', r'\1', existing_sql) # remove quotes
        tmp_sql = re.sub('"(.+?)"', r'\1', tmp_sql) # remove quotes
        return diff(existing_sql, tmp_sql)


    def process_views(self, views):
        """ returns a dict of discovered views. Going to each view and try to discover recursively all "FROM" dependanices
        """
        all_views = {}
        def _recursively_process_view(view):
            """ process recursively views and add all dependency views to the global dict
            """
            athena_type = None
            if self.query(f"SHOW VIEWS LIKE '{view}'", include_header=True):
                athena_type = 'view'
            elif self.query(f"SHOW TABLES LIKE '{view}'", include_header=True):
                athena_type = 'table'
            else:
                logger.debug(f'{view} not a view and not a table. Skipping.')
                return None
            cid_print(f"    Processing Athena {athena_type}: <BOLD>{view}<END>")

            all_views[view] = {}
            if athena_type == 'view':
                sql = self.query(f'SHOW CREATE VIEW {view}', include_header=True)
                if not sql:
                    return
                sql = '\n'.join([line[0] for line in sql])
                #print("sql", sql)
                all_views[view]["dependsOn"] = {}
                all_views[view]["dependsOn"]['views'] = []
                deps = re.findall(r'FROM\W+?([\w."]+)', sql)
                for dep_view in deps:
                    #FIXME: need to add cross Database Dependencies
                    if dep_view.upper() in ('SELECT', 'VALUES'): # remove "FROM SELECT" and "FROM VALUES"
                        continue
                    dep_view = dep_view.replace('"', '')
                    if len(dep_view.split('.')) == 2:
                        dep_database, dep_view_name = dep_view.split('.')
                    dep_view = dep_view.split('.')[-1]
                    if dep_view not in all_views:
                        _recursively_process_view(dep_view)
                    if dep_view not in all_views[view]["dependsOn"]['views'] and dep_view in all_views:
                        all_views[view]["dependsOn"]['views'].append(dep_view)
                if not all_views[view]["dependsOn"]['views']:
                    del all_views[view]["dependsOn"]

            elif athena_type == 'table':
                sql = self.query(f'SHOW CREATE TABLE {view}', include_header=True)
                sql = '\n'.join([line[0] for line in sql])

            all_views[view]['data'] = sql.rstrip()
            return all_views[view]

        for view in views:
            _recursively_process_view(view)

        return all_views

    def create_or_update_view(self, view_name, view_query):
        """ update view while asking user
        """
        update_view = None
        # first understand if view exists
        self.discover_views([view_name])
        logger.trace(str(list(self._metadata.keys())))
        if view_name not in self._metadata:
            update_view = True
        else: # view exists
            while get_parameters().get('on-drift', 'show').lower() != 'override' and isatty():
                cid_print(f'Analyzing view {view_name}')
                diff = self.get_view_diff(view_name, view_query)
                if diff and diff['diff']:
                    cid_print(f'<BOLD>Found a difference between existing view <YELLOW>{view_name}<END> <BOLD>and the one we want to deploy. <END>')
                    cid_print(diff['printable'])
                    choice = get_parameter(
                        param_name='view-' + view_name + '-override',
                        message=f'The existing view is different. Override?',
                        choices=['retry diff', 'proceed and override', 'keep existing', 'exit'],
                        default='retry diff',
                        yes_choice='proceed and override',
                        fuzzy=False,
                    )
                    if choice == 'retry diff':
                        unset_parameter('view-' + view_name + '-override')
                        continue
                    elif choice == 'proceed and override':
                        update_view = True
                        break
                    elif choice == 'keep existing':
                        update_view = False
                        break
                    else:
                        raise CidCritical(f'User choice is not to update {view_name}.')
                elif not diff:
                    if not get_yesno_parameter(
                        param_name='view-' + view_name + '-override',
                        message=f'Cannot get sql diff for {view_name}. Continue?',
                        default='yes'
                        ):
                        raise CidCritical(f'User choice is not to update {view_name}.')
                    update_view = True
                elif diff and not diff['diff']:
                    cid_print(f'No need to update {view_name}. Skipping.')
                break
        if update_view:
            cid_print(f'Updating view: "{view_name}"')
            self.execute_query(view_query)


    def find_tables_with_columns_in_information_schema(self, columns):
        """ find table and database that contain given set of columns
        this function takes 1+ min to fetch information schema. For better performance consider find_tables_with_columns.
        """
        columns = set(columns)
        return self.execute_query(f'''
        SELECT
            table_schema,
            table_name
        FROM
            information_schema.columns
        WHERE
            column_name IN ({[','.join(["'"+col+"'" for col in columns])]})
        GROUP BY
            table_schema,
            table_name
        HAVING
            COUNT(DISTINCT column_name) = {len(columns)}
        ''')

    def find_tables_with_columns(self, columns: list, database_name: str=None, catalog_name: str=None, max_items: int=10000):
        """ Returns an iterator that yields only tables containing all specified columns.
        """
        iterator = self.client.get_paginator('list_table_metadata').paginate(
            DatabaseName=database_name or self.DatabaseName,
            CatalogName=catalog_name or self.CatalogName,
            PaginationConfig= {
                'MaxItems': max_items, # sometimes customers can have 1'000s of tables (due to a crawler going crazy for example)
            },
        )
        # We cannot rely on search to find directly columns as there might be Nulls. So iterating old fashion.
        for table in iterator.search('TableMetadataList'):
            column_names = [c['Name'] for c in table.get('Columns', [])]
            if all([(col in column_names) for col in columns]):
                yield table

