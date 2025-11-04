""" Manage AWS CUR
"""
import json
import logging
from abc import abstractmethod

from cid.base import CidBase
from cid.utils import get_parameter, get_parameters, set_parameters, cid_print
from cid.exceptions import CidCritical
from cid.helpers.cur_proxy import ProxyView

from tqdm import tqdm

logger = logging.getLogger(__name__)


class AbstractCUR(CidBase):
    """ Manage AWS CUR
    """
    # Must be both CUR1 and 2 compatible
    cur_minimal_required_columns = [
        'bill_bill_type',
        'bill_billing_entity',
        'bill_billing_period_end_date',
        'bill_billing_period_start_date',
        'bill_invoice_id',
        'bill_payer_account_id',
        'identity_line_item_id',
        'identity_time_interval',
        'line_item_legal_entity',
        'line_item_line_item_description',
        'line_item_line_item_type',
        'line_item_operation',
        'line_item_product_code',
        'line_item_unblended_cost',
        'line_item_usage_account_id',
        'line_item_usage_amount',
        'line_item_usage_end_date',
        'line_item_usage_start_date',
        'line_item_usage_type',
        'pricing_term',
        'pricing_unit',
    ]
    ri_required_columns = [
        'reservation_reservation_a_r_n',
        'reservation_effective_cost',
        'reservation_start_time',
        'reservation_end_time',
        'pricing_lease_contract_length',
        'pricing_offering_class',
        'pricing_purchase_option'
    ]
    sp_required_columns = [
        'savings_plan_savings_plan_a_r_n',
        'savings_plan_savings_plan_effective_cost',
        'savings_plan_start_time',
        'savings_plan_end_time',
        'savings_plan_purchase_term',
        'savings_plan_offering_type',
        'savings_plan_payment_option'
    ]
    _metadata = None
    _database = None
    _tag_and_cost_category = None

    def __init__(self, athena, glue):
        self.athena = athena
        self.glue = glue
        self.proxy = None

    @property
    def database(self) -> str:
        """get Athena database of CUR """
        if self.metadata is None: # Need explicit call of metadata. Do not remove this line
            raise CidCritical('Error: Cannot detect any CUR table. Hint: Check if AWS Lake Formation is activated on your account, verify that the LakeFormationEnabled parameter is set to yes on the deployment stack')
        return self._database

    @property
    def table_name(self) -> str:
        """ Get Athena table name """
        if self.metadata is None: # Need explicit call of metadata. Do not remove this line
            raise CidCritical('Error: Cannot detect any CUR table. Hint: Check if AWS Lake Formation is activated on your account, verify that the LakeFormationEnabled parameter is set to yes on the deployment stack')
        return self.metadata.get('Name')

    @property
    def has_resource_ids(self) -> bool:
        """ Return True if CUR has resource ids """
        return 'line_item_resource_id' in self.fields

    @property
    def has_reservations(self) -> bool:
        """ Return True if CUR has reservation fields """
        return all(col in self.fields for col in self.ri_required_columns)

    @property
    def has_savings_plans(self) -> bool:
        """ Return True if CUR has savings plan """
        return all(col in self.fields for col in self.sp_required_columns)

    @property
    def version(self) -> str:
        """ Return version of CUR """
        return '2' if 'bill_payer_account_name' in self.fields else '1'

    def get_type_of_column(self, column: str, version=None):
        """ Return an Athena type of a given non existent CUR column """
        if column.startswith('cost_category_') or column.startswith('resource_tags_'):
            return 'STRING'
        for ending in ['_cost', '_factor', '_quantity', '_fee', '_amount', '_discount', '_usage', '_usage_ratio']:
            if column.endswith(ending):
                return 'DOUBLE'
        if column.endswith('_date') and not column.endswith('_to_date'):
            return 'TIMESTAMP'
        if column.endswith('_time') and (version or self.version) == '2':
            return 'STRING' # yes, they are string
        special_cases = {
            "cost_category": "MAP",
            "discount": "MAP",
            "product": "MAP",
            "resource_tags": "MAP",
            "reservation_amortized_upfront_fee_for_billing_period": "DOUBLE",
            "reservation_unused_amortized_upfront_fee_for_billing_period": "DOUBLE",
            "reservation_upfront_value": "DOUBLE",
            "reservation_net_amortized_upfront_fee_for_billing_period": "DOUBLE",
            "reservation_net_unused_amortized_upfront_fee_for_billing_period": "DOUBLE",
            "reservation_net_upfront_value": "DOUBLE",
            "savings_plan_total_commitment_to_date": "DOUBLE",
            "savings_plan_savings_plan_rate": "DOUBLE",
            "savings_plan_used_commitment": "DOUBLE",
            "savings_plan_amortized_upfront_commitment_for_billing_period": "DOUBLE",
            "savings_plan_net_amortized_upfront_commitment_for_billing_period": "DOUBLE",
            "savings_plan_recurring_commitment_for_billing_period": "DOUBLE",
        }
        return special_cases.get(column, 'STRING')

    def column_exists(self, column:  str):
        return column.lower() in [col.lower() for col in self.fields]

    def ensure_columns(self, columns):
        if not isinstance(columns, list):
            logger.debug(f'{columns} must be list')
            return
        for column in columns:
            self.ensure_column(column)

    @abstractmethod
    def ensure_column(self, column: str, column_type: str=None):
        """ Ensure column is in the cur. If it is not there - add column """
        pass

    def table_is_cur(self, table: dict=None, name: str=None, return_reason: bool=False, database: str=None) -> bool:
        """ return cur version if table metadata fits CUR definition. """
        try:
            table = table or self.athena.get_table_metadata(name, database)
        except Exception as exc: #pylint: disable=broad-exception-caught
            logger.warning(exc)
            return False if not return_reason else (False, f'cannot get table {name}. {exc}.')

        table_name = table.get('Name')
        columns = [col.get('Name') for col in table.get('Columns')]
        missing_columns = [col for col in self.cur_minimal_required_columns if col not in columns]
        logger.debug(f'missing_columns={missing_columns}')
        if missing_columns:
            return False if not return_reason else (False, f"Table {table_name} does not contain columns: {','.join(missing_columns)}. You can try ALTER TABLE {table_name} ADD COLUMNS (missing_column string).")

        version = '1'
        if 'bill_payer_account_name' in columns:
            version = '2'
        return version if not return_reason else (True, 'all good')

    @property
    def fields(self) -> list:
        """get CUR fields """
        return [col.get('Name') for col in (self.metadata.get('Columns', []) + self.metadata.get('PartitionKeys', []))]

    @property
    def tag_and_cost_category_fields(self) -> list:
        """ Returns all SQL selectable fields with tags and cost category."""
        if self.version == '1':
            return [field for field in self.fields if field.startswith('resource_tags_') or field.startswith('cost_category_')]
        elif self.version == '2':
            if self._tag_and_cost_category is not None: # the query can take few mins so we try to cache it
                logging.debug(f'Using cached tags.')
                return self._tag_and_cost_category

            self._tag_and_cost_category = []
            number_of_rows_scanned = 500000 # empiric value
            for tag_type in ['resource_tags', 'cost_category']:
                if tag_type not in self.fields:
                    logging.debug(f'skipping {tag_type} scan')
                cid_print(f'Scanning {tag_type} in {self.table_name}.')
                try:
                    res = self.athena.query(
                        sql=f'''
                            SELECT
                                key,
                                COUNT(DISTINCT value) as unique_values
                            FROM (
                                SELECT {tag_type}
                                FROM "{self.database}"."{self.table_name}"
                                WHERE billing_period >= DATE_FORMAT(DATE_ADD('day', -60, CURRENT_DATE), '%Y-%m')
                                AND line_item_usage_start_date > DATE_ADD('day', -60, CURRENT_DATE)
                                AND cardinality({tag_type}) > 0
                                LIMIT {number_of_rows_scanned}
                            ) t
                            CROSS JOIN UNNEST({tag_type}) AS t(key, value)
                            GROUP BY key
                            ORDER BY unique_values DESC;
                        ''',
                        database=self.database,
                    )
                    max_width = max(len(str(line[0])) for line in res)
                    cid_print(f' <BOLD>{tag_type:<{max_width}} | Distinct Values <END> ')
                    for line in res:
                        if int(line[1]) > 10:
                            name = line[0]
                            name = name.replace('user_', '')
                            cid_print(f' <BOLD>{name:<{max_width}}<END> | {line[1]} ')
                    self._tag_and_cost_category += sorted([f"{tag_type}['{line[0]}']" for line in res])
                except (self.athena.client.exceptions.ClientError, CidCritical, ValueError) as exc:
                    logger.error(f'Failed to read {tag_type} from {self.table_name}: "{exc}". Will continue without.')
            return self._tag_and_cost_category
        else:
            raise NotImplemented('cur version not known')


class CUR(AbstractCUR):
    """This Class represents CUR table (1 or 2 versions)"""

    def __init__(self, athena, glue, database: str=None, table: str=None):
        super().__init__(athena, glue)
        if database and table:
            self.set_cur(database, table)

    @property
    def metadata(self) -> dict:
        """get Athena metadata for the table of CUR """
        if self._metadata:
            return self._metadata
        self._database, self._metadata = self.find_cur()
        # good place to set a database for athena
        return self._metadata


    def set_cur(self, database: str=None, table: str=None):
        self._database, self._metadata = self.find_cur(database, table)

    def find_cur(self, database: str=None, table: str=None):
        """Choose CUR"""
        metadata = None
        cur_database = database or get_parameters().get('cur-database')
        if not table and get_parameters().get('cur-table-name') and ',' not in str(get_parameters().get('cur-table-name')):
            table = get_parameters().get('cur-table-name')
        if table: # Table specified explicitly, so no need to scan. Just need to make sure the table exists
            try:
                metadata = self.athena.get_table_metadata(table, cur_database or self.athena.DatabaseName)
            except self.athena.client.exceptions.MetadataException as exc:
                logger.debug(f'Provided cur-table-name "{table}" in database "{cur_database or self.athena.DatabaseName}" is not found. ({exc})')
                raise CidCritical(f'Provided cur-table-name "{table}" in database "{cur_database or self.athena.DatabaseName}" is not found. Please make sure the table exists. This could also indicate a LakeFormation permission issue, see our FAQ for help.')
            res, message = self.table_is_cur(table=metadata, return_reason=True)
            if not res:
                raise CidCritical(f'Provided cur-table-name "{table}" in database "{cur_database or self.athena.DatabaseName}" is not cur. {message}')
            return (cur_database or self.athena.DatabaseName), metadata

        all_cur_tables = []
        filter_names = None
        if ',' in str(get_parameters().get('cur-table-name')):
            filter_names = get_parameters().get('cur-table-name').split(',')
        if cur_database:
            cur_databases = [cur_database]
        else:
            cur_databases = list(self.athena.list_databases()) # user have not provided the cur database
        for database in tqdm(cur_databases, desc='Fining CUR in Athena Databases'):
            try:
                tables = self.athena.find_tables_with_columns(
                    columns=self.cur_minimal_required_columns,
                    database_name=database,
                )
                all_cur_tables += [
                    (database, table['Name'])
                    for table in tables
                    if (not filter_names or table['Name'] in filter_names)
                    and table['Name'] not in ('cur2_proxy', 'cur2_view')
                ]
            except self.athena.client.exceptions.ClientError as exc:
                if 'AccessDenied' in str(exc):
                    logger.info(f'Cannot read from athena database {database}')
                else:
                    raise

        if not all_cur_tables:
            # FIXME : distinguish a case where we have NONE tables in any database. This might be because lake formation
            raise CidCritical(
                f'CUR table not found in {self.athena.region}. We need a least one Glue table with these columns: {self.cur_minimal_required_columns}.'
                 ' Please make sure you have CUR, preferably with CID CloudFormation stack.'
                 ' Also if you have AWS Lake Formation, the user running the tool might need additional permissions'
            )
        databases = set([database for database, _ in all_cur_tables])
        if len(databases) > 1:
            choices = dict(sorted([(f'{database}.{tab}', (database, tab)) for database, tab in all_cur_tables], reverse=True))
        else:
            choices = dict(sorted([(f'{tab}', (database, tab)) for database, tab in all_cur_tables], reverse=True))
        if not choices and not get_parameters().get('no-cur-creation'):
            choices['<CREATE CUR TABLE AND CRAWLER>'] = '<CREATE CUR TABLE AND CRAWLER>'
        answer =  get_parameter(
            param_name='cur-table-name-and-db',
            message="Please select CUR table",
            choices=choices,
        )
        if answer == '<CREATE CUR TABLE AND CRAWLER>':
            raise CidCritical('CUR creation was requested') # to be captured in common.py
        if isinstance(answer, str) and '.' in answer and len(answer.split('.')) == 2:
            database, table = answer.split('.')
        else:
            database, table = answer
        set_parameters({'cur-table-name': table,'cur-database': database})
        return database, self.athena.get_table_metadata(table, database_name=database)

    def ensure_column(self, column: str, column_type: str=None):
        """ Ensure column is in the cur. If it is not there - add column """
        column = column.lower()
        column = column.split('[')[0]
        if self.column_exists(column):
            return
        logger.trace(f'trying to add to CUR following column: {column}')
        table_can_be_updated = False
        # Check Crawler Behavior - if it has a Configuration/CrawlerOutput/TablesAddOrUpdateBehavior != MergeNewColumns, it will override columns
        crawler_name = self.metadata.get('Parameters', {}).get('UPDATED_BY_CRAWLER')
        if crawler_name:
            try:
                crawler = self.glue.get_crawler(crawler_name)
                config = json.loads(crawler.get('Configuration', '{}'))
                add_or_update = config.get('CrawlerOutput', {}).get('Tables', {}).get('AddOrUpdateBehavior')
                if add_or_update == 'MergeNewColumns':
                    table_can_be_updated = True
            except self.glue.client.exceptions.ClientError as exc:
                logger.debug(f'Failed to get crawler info: {exc}')
        if table_can_be_updated:
            column_type = column_type or self.get_type_of_column(column)
            try:
                self.athena.query(f'ALTER TABLE {self.table_name} ADD COLUMNS ({column} {column_type})', database=self.database)
            except (self.athena.client.exceptions.ClientError, CidCritical) as exc:
                raise CidCritical(f'Column {column} is not found in CUR and we were unable to add it. Please check FAQ.') from exc
            # table takes time to update so just adding column to cached data
            self._metadata.get('Columns', []).append({'Name': column, 'Type': column_type})
            cid_print(f"Column '{column}' was added to CUR ({self.table_name}).")
            return

        # if table cannot be updated, check if it is ri/sp case - let's hope dashboard views can handle it:
        if column in self.sp_required_columns + self.ri_required_columns:
            logger.warn(f"Column '{column}' is not present in ({self.table_name}). Will try to continue without.")
            return

        # if a required column is not there and not ri/sp -> stop
        logger.warning(f"Column '{column}' is not in CUR ({self.table_name}).")



class ProxyCUR(AbstractCUR):
    """ Cur Proxy behaves as CUR (1 or 2) but actually is a proxy view in Athena
    """
    def __init__(self, cur, target_cur_version):
        self.proxy = []
        self.cur = cur
        self.target_cur_version = target_cur_version
        super().__init__(cur.athena, cur.glue)

    @property
    def database(self) -> str:
        return self.athena.DatabaseName # use CID database for proxy view

    @property
    def metadata(self) -> dict:
        """get Athena metadata for the view of CUR """
        if self._metadata:
            return self._metadata
        self.cur = CUR(self.athena, self.glue)
        if self.cur.version != self.target_cur_version:
            cid_print(f'CUR{self.cur.version} ({self.athena.DatabaseName}.{self.cur.table_name}) provided but {self.target_cur_version} is needed. CUR Proxy view will be used.')
        self.proxy = ProxyView(cur=self.cur, target_cur_version=self.target_cur_version)
        try:
            self._metadata = self.athena.get_table_metadata(self.proxy.name)
        except self.athena.client.exceptions.MetadataException: # if no table - create it
            self.proxy.create_or_update_view()
            self._metadata = self.athena.get_table_metadata(self.proxy.name)

        return self._metadata

    def ensure_columns(self, columns):
        if not self.metadata:# To make sure metadata exists
            raise RuntimeError('No metadata')
        if isinstance(columns, list):
            if all(self.proxy.column_surely_exist(col) for col in columns):
                return
            if self.cur.metadata.get('TableType') == 'EXTERNAL_TABLE':
                try:
                    equivalent_columns = sum([self.proxy.source_column_equivalents(col) for col in columns], [])
                    logger.trace(f'equivalent_columns = {dict(zip(columns,equivalent_columns))}')
                    self.cur.ensure_columns(list(set(equivalent_columns)))
                    # add field from underlying cur to the proxy
                    for item in self.cur._metadata.get('Columns', []):
                        for existing_item in self._metadata.get('Columns', []):
                            if item['Name'] == existing_item['Name']:
                                break
                        else: # if not found
                            self._metadata.get('Columns', []).append(item)
                except Exception as exc:
                    logger.exception(exc)
            for column in columns:
                self.proxy.fields_to_expose.append(column)
            self.proxy.create_or_update_view()
