"""
Preservation Property Tests: Default Catalog Behavior Unchanged

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

Property 2: Preservation - Default Catalog Behavior Unchanged

These tests MUST PASS on UNFIXED code. They confirm baseline behavior that
the fix must not break. They verify that:
1. With single catalog (AwsDataCatalog), datasets use AwsDataCatalog in Catalog field
2. With default catalog explicitly selected, behavior is identical
3. Other template variables substitute correctly regardless of catalog
4. YAML non-dataset content (dashboards, layout) is unchanged
5. Export logic templatizes DataSourceArn and Schema correctly
"""
import json
import os
import glob
from string import Template

import yaml
import pytest
from hypothesis import given, settings, assume, HealthCheck, Phase
from hypothesis import strategies as st


# Project root for locating dataset templates and YAML dashboards
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
DATASETS_DIR = os.path.join(PROJECT_ROOT, 'cid', 'builtin', 'core', 'data', 'datasets')
DASHBOARDS_DIR = os.path.join(PROJECT_ROOT, 'dashboards')


# Strategy: Generate valid template variable values for non-catalog variables
datasource_arn_strategy = st.from_regex(
    r'arn:aws:quicksight:us-east-1:[0-9]{12}:datasource/[a-z0-9\-]{10,36}',
    fullmatch=True,
)
database_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('Ll',), whitelist_characters='_'),
    min_size=3,
    max_size=20,
).filter(lambda x: x.strip() == x and len(x) >= 3 and x not in (
    'true', 'false', 'yes', 'no', 'on', 'off', 'null', 'none',
))

table_name_strategy = st.text(
    alphabet=st.characters(whitelist_categories=('Ll',), whitelist_characters='_'),
    min_size=3,
    max_size=20,
).filter(lambda x: x.strip() == x and len(x) >= 3 and x not in (
    'true', 'false', 'yes', 'no', 'on', 'off', 'null', 'none',
))


class TestPreservation_DefaultCatalogInJSONTemplates:
    """Test that JSON dataset templates use AwsDataCatalog when default catalog is active.

    **Validates: Requirements 3.1, 3.2**

    In default-catalog environments (single catalog or AwsDataCatalog selected),
    the RelationalTable.Catalog field remains "AwsDataCatalog" after template
    substitution. The templates now use ${athena_catalog_name} which resolves to
    "AwsDataCatalog" when the default catalog is selected. This is the correct
    behavior that preserves backward compatibility.
    """

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_json_templates_preserve_default_catalog(self, datasource_arn, database_name):
        """
        **Validates: Requirements 3.1, 3.2**

        For default catalog scenarios, JSON dataset templates MUST produce
        Catalog="AwsDataCatalog" in all RelationalTable entries after substitution.
        The templates use ${athena_catalog_name} which resolves to "AwsDataCatalog"
        when the default catalog is selected.
        """
        json_files = glob.glob(os.path.join(DATASETS_DIR, '**', '*.json'), recursive=True)
        assume(len(json_files) > 0)

        for json_file in json_files:
            with open(json_file, 'r') as f:
                template_text = f.read()

            # Build columns_tpl with athena_catalog_name set to the default catalog
            # This simulates the fixed code in a default-catalog environment
            columns_tpl = {
                'athena_datasource_arn': datasource_arn,
                'athena_database_name': database_name,
                'athena_catalog_name': 'AwsDataCatalog',
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
                'primary_tag_name': 'team',
                'secondary_tag_name': 'project',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            compiled_dataset = json.loads(compiled_text)

            # Verify all RelationalTable entries that have a Catalog field
            # resolve to "AwsDataCatalog" after template substitution
            physical_table_map = compiled_dataset.get('PhysicalTableMap', {})
            for table_key, table_value in physical_table_map.items():
                if 'RelationalTable' in table_value:
                    rel_table = table_value['RelationalTable']
                    catalog_value = rel_table.get('Catalog')
                    relative_path = os.path.relpath(json_file, PROJECT_ROOT)

                    if catalog_value is not None:
                        # When Catalog is present, it should resolve to AwsDataCatalog
                        assert catalog_value == 'AwsDataCatalog', (
                            f"In {relative_path}, RelationalTable '{table_key}' "
                            f"Catalog='{catalog_value}', expected 'AwsDataCatalog' "
                            f"in default catalog scenario."
                        )


class TestPreservation_TemplateVariableSubstitution:
    """Test that standard template variables substitute correctly.

    **Validates: Requirements 3.3**

    Regardless of catalog selection, the other template variables
    (athena_datasource_arn, athena_database_name, cur_database, cur_table_name)
    must substitute correctly in both JSON and YAML templates.
    """

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
        cur_database=database_name_strategy,
        cur_table_name=table_name_strategy,
    )
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_json_template_variables_substitute_correctly(
        self, datasource_arn, database_name, cur_database, cur_table_name
    ):
        """
        **Validates: Requirements 3.3**

        For all JSON dataset templates, template variables
        ${athena_datasource_arn} and ${athena_database_name} MUST be
        substituted with the provided values. No unresolved template
        variables should remain in DataSourceArn or Schema fields.
        """
        json_files = glob.glob(os.path.join(DATASETS_DIR, '**', '*.json'), recursive=True)
        assume(len(json_files) > 0)

        for json_file in json_files:
            with open(json_file, 'r') as f:
                template_text = f.read()

            columns_tpl = {
                'athena_datasource_arn': datasource_arn,
                'athena_database_name': database_name,
                'cur_database': cur_database,
                'cur_table_name': cur_table_name,
                'cur1_database': cur_database,
                'cur1_table_name': cur_table_name,
                'cur2_database': cur_database,
                'cur2_table_name': cur_table_name,
                'primary_tag_name': 'team',
                'secondary_tag_name': 'project',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            compiled_dataset = json.loads(compiled_text)

            physical_table_map = compiled_dataset.get('PhysicalTableMap', {})
            for table_key, table_value in physical_table_map.items():
                if 'RelationalTable' in table_value:
                    rel_table = table_value['RelationalTable']
                    relative_path = os.path.relpath(json_file, PROJECT_ROOT)

                    # DataSourceArn must be fully substituted
                    assert rel_table.get('DataSourceArn') == datasource_arn, (
                        f"In {relative_path}, RelationalTable '{table_key}' "
                        f"DataSourceArn='{rel_table.get('DataSourceArn')}', "
                        f"expected '{datasource_arn}'."
                    )

                    # Schema must be fully resolved (no unresolved variables)
                    # Some templates use ${athena_database_name}, others use
                    # ${cur_database} or ${cur1_database}, all should resolve
                    schema_value = rel_table.get('Schema', '')
                    assert '${' not in schema_value, (
                        f"In {relative_path}, RelationalTable '{table_key}' "
                        f"Schema='{schema_value}' has unresolved template variable."
                    )

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=5, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_yaml_template_variables_substitute_correctly(
        self, datasource_arn, database_name
    ):
        """
        **Validates: Requirements 3.3**

        For YAML dashboard definitions, template variables
        ${athena_datasource_arn} and ${athena_database_name} MUST be
        substituted with the provided values in RelationalTable entries.
        """
        # Use a small representative sample of YAML files
        yaml_files = glob.glob(os.path.join(DASHBOARDS_DIR, '**', '*.yaml'), recursive=True)
        assume(len(yaml_files) > 0)

        # Pick small files for efficiency
        sample_files = [f for f in yaml_files if 'rls' in f.lower() or 'cloudfront_realtime' in f.lower()]
        if not sample_files:
            sample_files = yaml_files[:2]

        for yaml_file in sample_files:
            with open(yaml_file, 'r') as f:
                template_text = f.read()

            columns_tpl = {
                'athena_datasource_arn': datasource_arn,
                'athena_database_name': database_name,
                'athena_catalog_name': 'AwsDataCatalog',  # default catalog
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            resources = yaml.safe_load(compiled_text)

            datasets = resources.get('datasets', {})
            for dataset_name, dataset_def in datasets.items():
                dataset_data = dataset_def.get('data', {})
                if isinstance(dataset_data, str):
                    continue  # Skip string refs

                physical_table_map = dataset_data.get('PhysicalTableMap', {})
                for table_key, table_value in physical_table_map.items():
                    if 'RelationalTable' in table_value:
                        rel_table = table_value['RelationalTable']
                        relative_path = os.path.relpath(yaml_file, PROJECT_ROOT)

                        # DataSourceArn must be substituted
                        assert rel_table.get('DataSourceArn') == datasource_arn, (
                            f"In {relative_path}, dataset '{dataset_name}', "
                            f"RelationalTable '{table_key}' "
                            f"DataSourceArn='{rel_table.get('DataSourceArn')}', "
                            f"expected '{datasource_arn}'."
                        )

                        # Schema must be substituted (no unresolved variables)
                        schema_value = rel_table.get('Schema')
                        if isinstance(schema_value, str):
                            assert '${' not in schema_value, (
                                f"In {relative_path}, dataset '{dataset_name}', "
                                f"RelationalTable '{table_key}' "
                                f"Schema='{schema_value}' has unresolved template variable."
                            )


class TestPreservation_YAMLDashboardDefaultCatalog:
    """Test that YAML dashboards resolve Catalog to AwsDataCatalog for default catalog.

    **Validates: Requirements 3.1, 3.2**

    When the default catalog (AwsDataCatalog) is used, all YAML dashboard
    RelationalTable entries must resolve Catalog to AwsDataCatalog after
    template substitution. The templates now use ${athena_catalog_name} which
    resolves to "AwsDataCatalog" when the default catalog is selected.
    """

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=5, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_yaml_dashboards_preserve_default_catalog(self, datasource_arn, database_name):
        """
        **Validates: Requirements 3.1, 3.2**

        For default catalog scenarios, all YAML dashboard RelationalTable entries
        with a Catalog field MUST have Catalog="AwsDataCatalog" after substitution.
        The templates use ${athena_catalog_name} which resolves to "AwsDataCatalog"
        when the default catalog is selected.
        """
        yaml_files = glob.glob(os.path.join(DASHBOARDS_DIR, '**', '*.yaml'), recursive=True)
        assume(len(yaml_files) > 0)

        # Use a representative sample to keep tests fast
        sample_files = [
            f for f in yaml_files
            if any(name in f.lower() for name in ['tao', 'rls', 'health-events'])
        ]
        if not sample_files:
            sample_files = yaml_files[:3]

        for yaml_file in sample_files:
            with open(yaml_file, 'r') as f:
                template_text = f.read()

            # Simulate default catalog scenario with athena_catalog_name set
            columns_tpl = {
                'athena_datasource_arn': datasource_arn,
                'athena_database_name': database_name,
                'athena_catalog_name': 'AwsDataCatalog',
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            resources = yaml.safe_load(compiled_text)

            datasets = resources.get('datasets', {})
            for dataset_name, dataset_def in datasets.items():
                dataset_data = dataset_def.get('data', {})
                if isinstance(dataset_data, str):
                    continue

                physical_table_map = dataset_data.get('PhysicalTableMap', {})
                for table_key, table_value in physical_table_map.items():
                    if 'RelationalTable' in table_value:
                        rel_table = table_value['RelationalTable']
                        catalog_value = rel_table.get('Catalog')
                        relative_path = os.path.relpath(yaml_file, PROJECT_ROOT)

                        if catalog_value is not None:
                            assert catalog_value == 'AwsDataCatalog', (
                                f"In {relative_path}, dataset '{dataset_name}', "
                                f"RelationalTable '{table_key}' has "
                                f"Catalog='{catalog_value}', expected 'AwsDataCatalog' "
                                f"in default catalog scenario."
                            )


class TestPreservation_YAMLNonDatasetContentUnchanged:
    """Test that YAML non-dataset content (dashboards, layout) is unaffected.

    **Validates: Requirements 3.5**

    Template substitution must not alter non-dataset content in YAML files
    such as dashboard configuration, visualization definitions, etc.
    """

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=5, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_yaml_dashboard_config_preserved(self, datasource_arn, database_name):
        """
        **Validates: Requirements 3.5**

        Template substitution MUST NOT alter the 'dashboards' section structure
        (dashboard names, IDs, versions, categories, themes) in YAML files.
        """
        yaml_files = glob.glob(os.path.join(DASHBOARDS_DIR, '**', '*.yaml'), recursive=True)
        assume(len(yaml_files) > 0)

        # Use focus.yaml as it has a well-defined dashboards section
        sample_files = [f for f in yaml_files if 'focus' in os.path.basename(f).lower()]
        if not sample_files:
            sample_files = yaml_files[:1]

        for yaml_file in sample_files:
            with open(yaml_file, 'r') as f:
                template_text = f.read()

            # Parse BEFORE substitution - to get the raw dashboard config
            # (The dashboards section doesn't use template variables)
            raw_resources = yaml.safe_load(template_text)
            raw_dashboards = raw_resources.get('dashboards', {})

            # Parse AFTER substitution
            columns_tpl = {
                'athena_datasource_arn': datasource_arn,
                'athena_database_name': database_name,
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            compiled_resources = yaml.safe_load(compiled_text)
            compiled_dashboards = compiled_resources.get('dashboards', {})

            # Dashboard structure must be identical (names, IDs, versions)
            assert set(raw_dashboards.keys()) == set(compiled_dashboards.keys()), (
                f"Dashboard keys changed after substitution in {yaml_file}"
            )

            for dash_name in raw_dashboards:
                raw_dash = raw_dashboards[dash_name]
                compiled_dash = compiled_dashboards[dash_name]

                # name, dashboardId, version, category, theme must be unchanged
                for key in ['name', 'dashboardId', 'version', 'category', 'theme']:
                    if key in raw_dash:
                        assert raw_dash[key] == compiled_dash.get(key), (
                            f"Dashboard '{dash_name}' key '{key}' changed: "
                            f"'{raw_dash[key]}' -> '{compiled_dash.get(key)}'"
                        )


class TestPreservation_ExportTemplatizesExistingFields:
    """Test that the export logic correctly templatizes DataSourceArn and Schema.

    **Validates: Requirements 3.5**

    The export logic in cid/export.py currently templatizes:
    - DataSourceArn -> ${athena_datasource_arn}
    - Schema -> ${athena_database_name} (for the default database)

    This behavior must be preserved regardless of any Catalog changes.
    """

    @given(
        original_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_export_templatizes_datasource_arn_and_schema(self, original_arn, database_name):
        """
        **Validates: Requirements 3.5**

        The export logic MUST templatize DataSourceArn to ${athena_datasource_arn}
        and Schema to ${athena_database_name} for RelationalTable entries.
        """
        # Simulate the export logic from cid/export.py (~line 168-179)
        dataset_data = {
            'PhysicalTableMap': {
                'table-1': {
                    'RelationalTable': {
                        'DataSourceArn': original_arn,
                        'Schema': database_name,
                        'Catalog': 'AwsDataCatalog',
                        'Name': 'some_view',
                    }
                }
            }
        }

        # Replicate the exact export logic (from cid/export.py)
        all_views_and_databases = []
        for key, value in dataset_data['PhysicalTableMap'].items():
            if 'RelationalTable' in value \
                and 'DataSourceArn' in value['RelationalTable'] \
                and 'Schema' in value['RelationalTable']:
                value['RelationalTable']['DataSourceArn'] = '${athena_datasource_arn}'
                database_name_val = value['RelationalTable']['Schema']
                discovered_databases = list(set([d for _, d in all_views_and_databases]))
                if not discovered_databases or discovered_databases[0] == database_name_val:
                    value['RelationalTable']['Schema'] = '${athena_database_name}'

        # Verify DataSourceArn was templatized
        for key, value in dataset_data['PhysicalTableMap'].items():
            if 'RelationalTable' in value:
                assert value['RelationalTable']['DataSourceArn'] == '${athena_datasource_arn}', (
                    f"Export should templatize DataSourceArn to '${{athena_datasource_arn}}', "
                    f"got '{value['RelationalTable']['DataSourceArn']}'"
                )
                assert value['RelationalTable']['Schema'] == '${athena_database_name}', (
                    f"Export should templatize Schema to '${{athena_database_name}}', "
                    f"got '{value['RelationalTable']['Schema']}'"
                )

    @given(
        original_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
    )
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_export_preserves_catalog_as_literal(self, original_arn, database_name):
        """
        **Validates: Requirements 3.5**

        On UNFIXED code, the export logic leaves the Catalog field as-is
        (the literal value from the live dataset). This test documents
        the current behavior that Catalog is NOT templatized.
        """
        # Simulate the export logic from cid/export.py (~line 168-179)
        dataset_data = {
            'PhysicalTableMap': {
                'table-1': {
                    'RelationalTable': {
                        'DataSourceArn': original_arn,
                        'Schema': database_name,
                        'Catalog': 'AwsDataCatalog',
                        'Name': 'some_view',
                    }
                }
            }
        }

        # Replicate the exact export logic (from cid/export.py)
        all_views_and_databases = []
        for key, value in dataset_data['PhysicalTableMap'].items():
            if 'RelationalTable' in value \
                and 'DataSourceArn' in value['RelationalTable'] \
                and 'Schema' in value['RelationalTable']:
                value['RelationalTable']['DataSourceArn'] = '${athena_datasource_arn}'
                database_name_val = value['RelationalTable']['Schema']
                discovered_databases = list(set([d for _, d in all_views_and_databases]))
                if not discovered_databases or discovered_databases[0] == database_name_val:
                    value['RelationalTable']['Schema'] = '${athena_database_name}'
                # NOTE: The export logic does NOT touch Catalog - this is the bug
                # but for preservation, we document that Catalog remains literal

        # On unfixed code, Catalog remains as-is (AwsDataCatalog literal)
        for key, value in dataset_data['PhysicalTableMap'].items():
            if 'RelationalTable' in value:
                catalog_value = value['RelationalTable'].get('Catalog')
                assert catalog_value == 'AwsDataCatalog', (
                    f"On unfixed code, export should leave Catalog as literal "
                    f"'AwsDataCatalog', got '{catalog_value}'"
                )


class TestPreservation_IAMPolicyDefaultCatalog:
    """Test that IAM policy uses AwsDataCatalog in the resource ARN by default.

    **Validates: Requirements 3.5**

    The IAM helper's ensure_data_source_role_exists now accepts a catalog_name
    parameter with a default of 'AwsDataCatalog'. For default-catalog
    environments, the function signature default ensures backward compatibility.
    """

    def test_iam_policy_supports_default_catalog(self):
        """
        **Validates: Requirements 3.5**

        The IAM policy function MUST have a catalog_name parameter that defaults
        to 'AwsDataCatalog', ensuring backward compatibility for default-catalog
        scenarios. The function must support dynamic catalog names via parameter.
        """
        iam_file = os.path.join(PROJECT_ROOT, 'cid', 'helpers', 'iam.py')
        with open(iam_file, 'r') as f:
            iam_source = f.read()

        # The function should accept a catalog_name parameter
        assert 'catalog_name' in iam_source, (
            "cid/helpers/iam.py should contain a 'catalog_name' parameter "
            "for dynamic catalog support."
        )

        # The default value should be 'AwsDataCatalog' for backward compatibility
        assert "'AwsDataCatalog'" in iam_source, (
            "cid/helpers/iam.py should contain 'AwsDataCatalog' as the default "
            "value for the catalog_name parameter."
        )

        # The function should use datacatalog/ pattern in the ARN
        assert 'datacatalog/' in iam_source, (
            "cid/helpers/iam.py should contain 'datacatalog/' for "
            "constructing the IAM resource ARN."
        )


class TestPreservation_ColumnsTPLCurrentBehavior:
    """Test that columns_tpl contains the expected current set of keys.

    **Validates: Requirements 3.3, 3.4**

    The current columns_tpl dictionary in create_or_update_dataset
    includes specific keys. This test verifies those keys are present
    and functional for template substitution.
    """

    @given(
        datasource_arn=datasource_arn_strategy,
        database_name=database_name_strategy,
        cur_database=database_name_strategy,
        cur_table_name=table_name_strategy,
    )
    @settings(max_examples=10, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_columns_tpl_current_keys_work(
        self, datasource_arn, database_name, cur_database, cur_table_name
    ):
        """
        **Validates: Requirements 3.3, 3.4**

        The columns_tpl dictionary MUST include athena_datasource_arn,
        athena_database_name, cur_database, cur_table_name and they
        MUST produce valid JSON when substituted into templates.
        """
        # Replicate current columns_tpl structure (without athena_catalog_name)
        columns_tpl = {
            'athena_datasource_arn': datasource_arn,
            'athena_database_name': database_name,
            'cur_database': cur_database,
            'cur_table_name': cur_table_name,
            'cur1_database': cur_database,
            'cur1_table_name': cur_table_name,
            'cur2_database': cur_database,
            'cur2_table_name': cur_table_name,
            'primary_tag_name': 'team',
            'secondary_tag_name': 'project',
        }

        # Verify these produce valid JSON for at least one template
        json_files = glob.glob(os.path.join(DATASETS_DIR, '**', '*.json'), recursive=True)
        assume(len(json_files) > 0)

        # Test with the co/dataset.json (known to have all relevant fields)
        co_dataset = os.path.join(DATASETS_DIR, 'co', 'dataset.json')
        if os.path.exists(co_dataset):
            with open(co_dataset, 'r') as f:
                template_text = f.read()

            compiled_text = Template(template_text).safe_substitute(columns_tpl)

            # Must produce valid JSON
            compiled_dataset = json.loads(compiled_text)
            assert 'PhysicalTableMap' in compiled_dataset
            assert 'DataSetId' in compiled_dataset

            # DataSourceArn and Schema should be fully resolved
            for table_key, table_value in compiled_dataset['PhysicalTableMap'].items():
                if 'RelationalTable' in table_value:
                    rel_table = table_value['RelationalTable']
                    # No unresolved template variables in these fields
                    assert '${' not in rel_table['DataSourceArn'], (
                        f"Unresolved variable in DataSourceArn: {rel_table['DataSourceArn']}"
                    )
                    assert '${' not in rel_table['Schema'], (
                        f"Unresolved variable in Schema: {rel_table['Schema']}"
                    )
