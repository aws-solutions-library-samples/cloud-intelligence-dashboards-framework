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

# Canonical Athena SQL type for each normalized type name.
_ATHENA_TYPE = {
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

# Types that are considered compatible (no cast needed)
_COMPATIBLE_TYPES = {
    'varchar': {'varchar', 'string'},
    'double': {'double', 'float'},
    'timestamp': {'timestamp'},
    'date': {'date'},
    'bigint': {'bigint', 'int', 'integer'},
    'boolean': {'boolean'},
    'map<varchar,varchar>': {'map<varchar,varchar>', 'map(varchar,varchar)', 'map(varchar, varchar)', 'map<string,string>'},
}


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

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
    return source_norm in _COMPATIBLE_TYPES.get(target_norm, {target_norm})


def _resolve_athena_type(type_str):
    """Resolve a type string to its canonical Athena SQL name (for CAST)."""
    norm = _normalize_type(type_str)
    if norm in _ATHENA_TYPE:
        return _ATHENA_TYPE[norm]
    # Safety net for map types not explicitly listed
    if norm.startswith('map'):
        return 'MAP<VARCHAR,VARCHAR>'
    return 'VARCHAR'


def _null_as(target_type):
    """Return a ``CAST(NULL AS <type>)`` expression for *target_type*."""
    return f'CAST(NULL AS {_resolve_athena_type(target_type)})'



# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class FocusConsolidationView:
    """Dynamically generates a FOCUS consolidation view by discovering
    and unioning all FOCUS tables across Athena databases.

    Follows the same pattern as ProxyView in cur_proxy.py.
    """

    def __init__(self, athena, columns):
        self.athena = athena
        self.name = 'focus_consolidation_view'
        self.columns = columns

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_focus_tables(self):
        """Discover all FOCUS tables across all Athena databases.

        Returns a list of dicts::

            {
                'database': str,
                'table_name': str,
                'columns': dict[str, str],   # column_name -> type
                'partition_keys': list[str],
            }
        """
        try:
            databases = self.athena.list_databases()
        except Exception as exc:
            logger.error(f'Failed to list databases: {exc}')
            return []

        logger.info(f'Scanning {len(databases)} databases for FOCUS tables')

        tables = []
        for db_name in databases:
            tables.extend(self._scan_database(db_name))

        logger.info(f'Discovered {len(tables)} FOCUS table(s) total')
        return tables

    def _scan_database(self, db_name):
        """Scan a single database for FOCUS tables."""
        tables = []
        try:
            for table_meta in self.athena.find_tables_with_columns(
                columns=FOCUS_MINIMUM_COLUMNS,
                database_name=db_name,
            ):
                table_info = self._parse_table_meta(db_name, table_meta)
                if table_info:
                    tables.append(table_info)
        except Exception as exc:
            # AccessDenied or other errors — skip this database
            logger.warning(f'Failed to scan database "{db_name}" for FOCUS tables: {exc}')
        return tables

    def _parse_table_meta(self, db_name, table_meta):
        """Parse Glue table metadata into a table_info dict, or None to skip."""
        table_name = table_meta.get('Name', '')

        # Only include actual tables, not views
        if table_meta.get('TableType') != 'EXTERNAL_TABLE':
            logger.debug(f'Skipping non-table: {db_name}.{table_name} (type={table_meta.get("TableType")})')
            return None

        # Exclude the consolidation view itself
        if table_name.lower() == self.name:
            logger.debug(f'Skipping self: {db_name}.{table_name}')
            return None

        # Build columns dict (name -> type)
        columns = {
            col['Name'].lower(): col.get('Type', 'varchar').lower()
            for col in table_meta.get('Columns', [])
        }

        # Add partition keys to columns dict if not already present
        partition_keys = []
        for pk in table_meta.get('PartitionKeys', []):
            pk_name = pk['Name'].lower()
            partition_keys.append(pk_name)
            columns.setdefault(pk_name, pk.get('Type', 'varchar').lower())

        logger.info(f'Found FOCUS table: "{db_name}"."{table_name}" ({len(columns)} columns)')
        return {
            'database': db_name,
            'table_name': table_name,
            'columns': columns,
            'partition_keys': partition_keys,
        }

    # ------------------------------------------------------------------
    # SQL generation
    # ------------------------------------------------------------------

    def generate_select_for_table(self, table_info):
        """Generate a SELECT statement for a single source table,
        mapping its columns to the FOCUS 1.2 target schema.
        """
        source_columns = table_info['columns']
        partition_keys = table_info.get('partition_keys', [])
        database = table_info['database']
        table_name = table_info['table_name']

        expressions = []
        for col_name in sorted(self.columns):
            target_type = self.columns[col_name]
            expr = self._column_expression(col_name, target_type, source_columns, partition_keys)
            expressions.append(f'  {expr}')

        select_block = '\n, '.join(expressions)
        return f'SELECT\n{select_block}\nFROM\n  "{database}"."{table_name}"'

    def _column_expression(self, col_name, target_type, source_columns, partition_keys):
        """Return the SQL expression for a single column in the SELECT list."""
        # Special case: billing_period
        if col_name == 'billing_period':
            return f'{self._billing_period_expr(source_columns, partition_keys)} {col_name}'

        # Column exists in source
        if col_name in source_columns:
            source_type = source_columns[col_name]
            if _types_compatible(source_type, target_type):
                return col_name
            # Array to scalar is not safely convertible — use NULL
            if _normalize_type(source_type).startswith('array'):
                return f'{_null_as(target_type)} {col_name}'
            # Types differ — cast
            return f'CAST({col_name} AS {_resolve_athena_type(target_type)}) {col_name}'

        # Column missing — typed NULL placeholder
        return f'{_null_as(target_type)} {col_name}'

    @staticmethod
    def _billing_period_expr(source_columns, partition_keys):
        """SQL expression for billing_period.

        Target format is YYYY-MM (e.g. '2026-02') to match AWS FOCUS convention.
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
        """Generate the full CREATE OR REPLACE VIEW SQL with UNION ALL."""
        if not tables:
            return ''

        select_blocks = [self.generate_select_for_table(t) for t in tables]
        union_sql = '\n\nUNION ALL\n\n'.join(select_blocks)
        return f'CREATE OR REPLACE VIEW "{self.name}" AS\n{union_sql}\n'

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

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

        selected_tables = [t for t, label in zip(tables, table_labels) if label in selected_labels]
        logger.info(f'Creating focus_consolidation_view from: {", ".join(selected_labels)}')

        sql = self.generate_view_sql(selected_tables)
        logger.debug(f'Generated SQL:\n{sql}')

        if not self._confirm_view_update(sql, cid_print, get_parameter, get_yesno_parameter, unset_parameter, isatty):
            return False

        self.athena.execute_query(sql)
        logger.info('focus_consolidation_view created/updated successfully')
        return True

    def _confirm_view_update(self, sql, cid_print, get_parameter, get_yesno_parameter, unset_parameter, isatty):
        """Show diff and ask for confirmation (same pattern as athena.py view update flow).

        Returns True to proceed with execution, False to abort.
        """
        while isatty():
            cid_print(f'Analyzing view {self.name}')
            view_diff = self.athena.get_view_diff(self.name, sql)

            # View doesn't exist yet or diff failed — ask to proceed
            if not view_diff:
                return get_yesno_parameter(
                    param_name='view-' + self.name + '-override',
                    message=f'Cannot get sql diff for {self.name}. Continue?',
                    default='yes',
                )

            # No diff — view is already up to date
            if not view_diff['diff']:
                cid_print(f'No need to update {self.name}. Skipping.')
                return False  # nothing to execute, but not an error

            # Diff exists — show it and let user decide
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
            if choice == 'proceed and override':
                return True
            if choice == 'keep existing':
                logger.info('User chose to keep existing focus_consolidation_view.')
                return False
            # exit
            logger.info('User chose to exit.')
            return False

        # Non-interactive — proceed
        return True
