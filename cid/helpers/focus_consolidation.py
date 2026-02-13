"""
Dynamic FOCUS Consolidation View Generator

Discovers FOCUS tables across all Athena databases and generates a consolidated
view with UNION ALL. Handles FOCUS 1.0, 1.1, and 1.2 tables by mapping columns
to the FOCUS 1.2 target schema, using NULL placeholders for missing columns and
type casting where needed.

Follows the same pattern as ProxyView in cur_proxy.py.
"""

import logging

logger = logging.getLogger(__name__)

# Minimum columns that identify a table as FOCUS-compliant (FOCUS 1.0 core).
# A table must have ALL of these columns (case-insensitive) to be considered a FOCUS table.
# NOTE: Only columns common across ALL providers (AWS, Azure, OCI, GCP) are listed here.
# Provider-specific renames (e.g. OCI uses 'provider' instead of 'providername') are
# handled during view generation, not during discovery.
FOCUS_MINIMUM_COLUMNS = [
    'billedcost',
    'billingaccountid',
    'billingcurrency',
    'billingperiodstart',
    'chargecategory',
    'chargedescription',
    'chargeperiodstart',
    'commitmentdiscountcategory',
    'commitmentdiscountid',
    'commitmentdiscountname',
    'effectivecost',
    'listcost',
    'listunitprice',
    'pricingcategory',
    'pricingquantity',
    'pricingunit',
    'servicecategory',
    'servicename',
    'skuid',
    'skupriceid',
    'subaccountid',
]

# Typed NULL expressions for Athena SQL, keyed by normalized type name.
NULL_EXPRESSIONS = {
    'varchar': 'CAST(NULL AS VARCHAR)',
    'string': 'CAST(NULL AS VARCHAR)',
    'double': 'CAST(NULL AS DOUBLE)',
    'float': 'CAST(NULL AS DOUBLE)',
    'timestamp': 'CAST(NULL AS TIMESTAMP)',
    'date': 'CAST(NULL AS DATE)',
    'bigint': 'CAST(NULL AS BIGINT)',
    'int': 'CAST(NULL AS BIGINT)',
    'integer': 'CAST(NULL AS BIGINT)',
    'boolean': 'CAST(NULL AS BOOLEAN)',
    'map<varchar,varchar>': 'CAST(NULL AS MAP<VARCHAR,VARCHAR>)',
    'map(varchar, varchar)': 'CAST(NULL AS MAP<VARCHAR,VARCHAR>)',
    'map<string,string>': 'CAST(NULL AS MAP<VARCHAR,VARCHAR>)',
}

# Types that are considered compatible (no cast needed)
COMPATIBLE_TYPES = {
    'varchar': {'varchar', 'string'},
    'double': {'double', 'float'},
    'timestamp': {'timestamp'},
    'date': {'date'},
    'bigint': {'bigint', 'int', 'integer'},
    'boolean': {'boolean'},
    'map<varchar,varchar>': {'map<varchar,varchar>', 'map(varchar,varchar)', 'map(varchar, varchar)', 'map<string,string>'},
}

# Athena type name to use in CAST expressions
CAST_TYPE_MAP = {
    'varchar': 'VARCHAR',
    'string': 'VARCHAR',
    'double': 'DOUBLE',
    'float': 'DOUBLE',
    'timestamp': 'TIMESTAMP',
    'date': 'DATE',
    'bigint': 'BIGINT',
    'int': 'BIGINT',
    'integer': 'BIGINT',
    'boolean': 'BOOLEAN',
    'map<varchar,varchar>': 'MAP<VARCHAR,VARCHAR>',
    'map(varchar,varchar)': 'MAP<VARCHAR,VARCHAR>',
    'map<string,string>': 'MAP<VARCHAR,VARCHAR>',
}


def _normalize_type(type_str):
    """Normalize an Athena type string for comparison."""
    if not type_str:
        return 'varchar'
    normalized = type_str.lower().strip().replace(' ', '')
    # Normalize map parentheses to angle brackets: map(x,y) -> map<x,y>
    if normalized.startswith('map(') and normalized.endswith(')'):
        normalized = 'map<' + normalized[4:-1] + '>'
    return normalized


def _types_compatible(source_type, target_type):
    """Check if source_type is compatible with target_type (no cast needed)."""
    source_norm = _normalize_type(source_type)
    target_norm = _normalize_type(target_type)
    # All map types are compatible with each other (no cast needed)
    if source_norm.startswith('map') and target_norm.startswith('map'):
        return True
    compatible_set = COMPATIBLE_TYPES.get(target_norm, {target_norm})
    return source_norm in compatible_set


def _get_cast_type(target_type):
    """Get the Athena SQL type name for CAST expressions."""
    norm = _normalize_type(target_type)
    if norm in CAST_TYPE_MAP:
        return CAST_TYPE_MAP[norm]
    # Safety net for map types not explicitly listed
    if norm.startswith('map<') or norm.startswith('map('):
        return 'MAP<VARCHAR,VARCHAR>'
    return 'VARCHAR'


def _get_null_expression(target_type):
    """Get a typed NULL expression for the given target type."""
    norm = _normalize_type(target_type)
    if norm in NULL_EXPRESSIONS:
        return NULL_EXPRESSIONS[norm]
    # Safety net for map types not explicitly listed
    if norm.startswith('map<') or norm.startswith('map('):
        return 'CAST(NULL AS MAP<VARCHAR,VARCHAR>)'
    return 'CAST(NULL AS VARCHAR)'


class FocusConsolidationView:
    """Dynamically generates a FOCUS consolidation view by discovering
    and unioning all FOCUS tables across Athena databases.

    Follows the same pattern as ProxyView in cur_proxy.py.
    """

    def __init__(self, athena, columns):
        self.athena = athena
        self.name = 'focus_consolidation_view'
        self.columns = columns

    def discover_focus_tables(self):
        """Discover all FOCUS tables across all Athena databases.

        Returns a list of dicts:
            {
                'database': str,
                'table_name': str,
                'columns': dict[str, str],  # column_name -> type
                'partition_keys': list[str],
            }
        """
        tables = []
        try:
            databases = self.athena.list_databases()
        except Exception as exc:
            logger.error(f'Failed to list databases: {exc}')
            return tables

        logger.info(f'Scanning {len(databases)} databases for FOCUS tables')

        for db_name in databases:
            try:
                for table_meta in self.athena.find_tables_with_columns(
                    columns=FOCUS_MINIMUM_COLUMNS,
                    database_name=db_name,
                ):
                    table_name = table_meta.get('Name', '')

                    # Only include actual tables, not views
                    if table_meta.get('TableType') != 'EXTERNAL_TABLE':
                        logger.debug(f'Skipping non-table: {db_name}.{table_name} (type={table_meta.get("TableType")})')
                        continue

                    # Exclude the consolidation view itself
                    if table_name.lower() == self.name:
                        logger.debug(f'Skipping self: {db_name}.{table_name}')
                        continue

                    # Build columns dict (name -> type) from Columns metadata
                    columns = {
                        col['Name'].lower(): col.get('Type', 'varchar').lower()
                        for col in table_meta.get('Columns', [])
                    }

                    # Partition keys
                    partition_keys = [
                        pk['Name'].lower()
                        for pk in table_meta.get('PartitionKeys', [])
                    ]

                    # Also add partition keys to columns dict if not already there
                    for pk in table_meta.get('PartitionKeys', []):
                        pk_name = pk['Name'].lower()
                        if pk_name not in columns:
                            columns[pk_name] = pk.get('Type', 'varchar').lower()

                    tables.append({
                        'database': db_name,
                        'table_name': table_name,
                        'columns': columns,
                        'partition_keys': partition_keys,
                    })
                    logger.info(f'Found FOCUS table: "{db_name}"."{table_name}" ({len(columns)} columns)')

            except Exception as exc:
                # AccessDenied or other errors - skip this database
                logger.warning(f'Failed to scan database "{db_name}" for FOCUS tables: {exc}')
                continue

        logger.info(f'Discovered {len(tables)} FOCUS table(s) total')
        return tables

    def generate_select_for_table(self, table_info):
        """Generate a SELECT statement for a single source table,
        mapping its columns to the FOCUS 1.2 target schema.

        Args:
            table_info: dict with keys: database, table_name, columns, partition_keys

        Returns:
            str: SQL SELECT statement
        """
        source_columns = table_info['columns']
        partition_keys = table_info.get('partition_keys', [])
        database = table_info['database']
        table_name = table_info['table_name']

        expressions = []
        # Process columns in sorted order for consistency
        for col_name in sorted(self.columns.keys()):
            target_type = self.columns[col_name]

            # Special case: billing_period
            if col_name == 'billing_period':
                expr = self._get_billing_period_expression(source_columns, partition_keys)
                expressions.append(f'  {expr} {col_name}')
                continue

            # Check if column exists in source
            if col_name in source_columns:
                source_type = source_columns[col_name]
                source_norm = _normalize_type(source_type)
                if _types_compatible(source_type, target_type):
                    # Types match - use directly
                    expressions.append(f'  {col_name}')
                elif source_norm.startswith('array'):
                    # Array to scalar is not safely convertible — use NULL
                    null_expr = _get_null_expression(target_type)
                    expressions.append(f'  {null_expr} {col_name}')
                else:
                    # Types differ - cast
                    cast_type = _get_cast_type(target_type)
                    expressions.append(f'  CAST({col_name} AS {cast_type}) {col_name}')
            else:
                # Column missing - use typed NULL
                null_expr = _get_null_expression(target_type)
                expressions.append(f'  {null_expr} {col_name}')

        select_block = '\n, '.join(expressions)
        return f'SELECT\n{select_block}\nFROM\n  "{database}"."{table_name}"'

    def _get_billing_period_expression(self, source_columns, partition_keys):
        """Get the SQL expression for billing_period column.

        Target format is YYYY-MM (e.g. '2026-02') to match AWS FOCUS convention.
        If billing_period exists but is a date type (e.g. Azure: 2026-02-01),
        convert it to YYYY-MM string format.
        """
        if 'billing_period' in source_columns or 'billing_period' in partition_keys:
            source_type = source_columns.get('billing_period', 'string')
            if _types_compatible(source_type, 'string'):
                return 'billing_period'
            # billing_period is a date or timestamp — format as YYYY-MM
            return "date_format(CAST(billing_period AS DATE), '%Y-%m')"
        # billing_period absent — compute from billingperiodstart
        return "date_format(CAST(billingperiodstart AS DATE), '%Y-%m')"

    def generate_view_sql(self, tables):
        """Generate the full CREATE OR REPLACE VIEW SQL with UNION ALL.

        Args:
            tables: list of table_info dicts from discover_focus_tables()

        Returns:
            str: Complete SQL statement
        """
        if not tables:
            return ''

        select_blocks = []
        for table_info in tables:
            select_blocks.append(self.generate_select_for_table(table_info))

        union_sql = '\n\nUNION ALL\n\n'.join(select_blocks)

        return f'CREATE OR REPLACE VIEW "{self.name}" AS\n{union_sql}\n'

    def create_or_update_view(self):
        """Main entry point. Discovers tables, generates SQL, executes it.

        Returns:
            bool: True if view was created/updated, False if no tables found.
        """
        from cid.utils import get_parameter, get_yesno_parameter, unset_parameter, cid_print, isatty

        tables = self.discover_focus_tables()

        if not tables:
            logger.warning('No FOCUS tables discovered. Cannot create focus_consolidation_view.')
            return False

        # Let user select which tables to include
        table_labels = [f'"{t["database"]}"."{t["table_name"]}"' for t in tables]
        selected_labels = get_parameter(
            param_name='focus-tables',
            message='Select FOCUS tables to include in the consolidation view',
            choices=table_labels,
            default=[],
            multi=True,
        )

        if not selected_labels:
            logger.warning('No FOCUS tables selected. Cannot create focus_consolidation_view.')
            return False

        # Filter tables to only selected ones
        selected_tables = [t for t, label in zip(tables, table_labels) if label in selected_labels]

        logger.info(f'Creating focus_consolidation_view from: {", ".join(selected_labels)}')

        sql = self.generate_view_sql(selected_tables)
        logger.debug(f'Generated SQL:\n{sql}')

        # Show diff and ask for confirmation (same pattern as athena.py view update flow)
        while isatty():
            cid_print(f'Analyzing view {self.name}')
            view_diff = self.athena.get_view_diff(self.name, sql)
            if view_diff and view_diff['diff']:
                cid_print(f'<BOLD>Found a difference between existing view <YELLOW>{self.name}<END> <BOLD>and the one we want to deploy. <END>')
                cid_print(view_diff['printable'])
                choice = get_parameter(
                    param_name='view-' + self.name + '-override',
                    message='The existing view is different. Override?',
                    choices=['retry diff', 'proceed and override', 'keep existing', 'exit'],
                    default='retry diff',
                    yes_choice='proceed and override',
                    fuzzy=False,
                )
                if choice == 'retry diff':
                    unset_parameter('view-' + self.name + '-override')
                    continue
                elif choice == 'proceed and override':
                    break
                elif choice == 'keep existing':
                    logger.info('User chose to keep existing focus_consolidation_view.')
                    return True
                else:
                    logger.info('User chose to exit.')
                    return False
            elif not view_diff:
                # View doesn't exist yet or diff failed — proceed
                if not get_yesno_parameter(
                    param_name='view-' + self.name + '-override',
                    message=f'Cannot get sql diff for {self.name}. Continue?',
                    default='yes',
                ):
                    return False
                break
            else:
                # No diff — view is already up to date
                cid_print(f'No need to update {self.name}. Skipping.')
                return True

        self.athena.execute_query(sql)
        logger.info('focus_consolidation_view created/updated successfully')
        return True
