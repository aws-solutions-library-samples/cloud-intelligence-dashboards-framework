"""
Bug Condition Exploration Test: Selected Catalog Ignored in Dataset Creation

**Validates: Requirements 1.1, 1.2, 1.3, 1.4**

Property 1: Bug Condition - Selected Catalog Ignored in Dataset Creation

This test validates the fix is in place. It verifies that when a non-default catalog is selected:
1. `columns_tpl` includes `athena_catalog_name` with the selected catalog value
2. Compiled JSON datasets use the selected catalog in RelationalTable.Catalog
3. YAML dashboard definitions use the selected catalog in RelationalTable.Catalog
4. IAM policy resource ARN references the selected catalog (not hardcoded AwsDataCatalog)
5. Export logic templatizes the Catalog field to ${athena_catalog_name}
"""
import json
import os
import re
from string import Template

import yaml
import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st


# Project root for locating dataset templates and YAML dashboards
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
DATASETS_DIR = os.path.join(PROJECT_ROOT, 'cid', 'builtin', 'core', 'data', 'datasets')
DASHBOARDS_DIR = os.path.join(PROJECT_ROOT, 'dashboards')


# Strategy: Generate non-default catalog names that start with a letter
# (avoids YAML interpreting purely numeric strings as integers)
catalog_name_strategy = st.from_regex(
    r'[A-Z][A-Za-z0-9_-]{2,29}', fullmatch=True
).filter(lambda x: x != 'AwsDataCatalog')


class TestBugCondition_ColumnsTPLMissingCatalog:
    """Test that columns_tpl dictionary includes athena_catalog_name.

    This test reads the actual cid/common.py source to verify the fix is in place.
    """

    @given(catalog_name=catalog_name_strategy)
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_columns_tpl_contains_athena_catalog_name(self, catalog_name):
        """
        **Validates: Requirements 1.2**

        The columns_tpl dictionary in cid/common.py MUST contain an
        'athena_catalog_name' key that references self.athena.CatalogName.
        """
        # Read the actual cid/common.py source code
        common_py_path = os.path.join(PROJECT_ROOT, 'cid', 'common.py')
        with open(common_py_path, 'r') as f:
            source = f.read()

        # Verify that 'athena_catalog_name' is present in the columns_tpl definition
        assert "'athena_catalog_name'" in source, (
            "cid/common.py does not contain 'athena_catalog_name' key in columns_tpl. "
            f"Selected catalog '{catalog_name}' would be ignored during template substitution."
        )

        # Verify it references self.athena.CatalogName
        assert 'self.athena.CatalogName' in source, (
            "cid/common.py does not reference self.athena.CatalogName. "
            "The athena_catalog_name template variable is not bound to the selected catalog."
        )

        # Verify the specific pattern: 'athena_catalog_name': self.athena.CatalogName
        pattern = r"'athena_catalog_name'\s*:\s*self\.athena\.CatalogName"
        assert re.search(pattern, source), (
            "cid/common.py does not have 'athena_catalog_name': self.athena.CatalogName "
            "in the columns_tpl dictionary."
        )


class TestBugCondition_JSONDatasetTemplatesHardcodedCatalog:
    """Test that JSON dataset templates use template variable for Catalog field.

    This test loads the 6 specific JSON dataset files that were fixed and verifies
    that RelationalTable definitions use ${athena_catalog_name} instead of
    hardcoded 'AwsDataCatalog'.
    """

    # Only check the 6 specific files that were in scope for the fix
    FIXED_JSON_FILES = [
        'co/dataset.json',
        'kpi/kpi_instance_all.json',
        'kpi/kpi_tracker.json',
        'kpi/kpi_ebs_storage_all.json',
        'kpi/kpi_ebs_snap.json',
        'kpi/kpi_s3_storage_all.json',
    ]

    @given(catalog_name=catalog_name_strategy)
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_json_templates_use_catalog_variable(self, catalog_name):
        """
        **Validates: Requirements 1.3, 1.4**

        For any non-default catalog, after template substitution, all
        RelationalTable.Catalog fields in the 6 fixed JSON datasets MUST
        equal the selected catalog.
        """
        for relative_file in self.FIXED_JSON_FILES:
            json_file = os.path.join(DATASETS_DIR, relative_file)
            assert os.path.exists(json_file), f"Expected file not found: {relative_file}"

            with open(json_file, 'r') as f:
                template_text = f.read()

            # Build columns_tpl with the non-default catalog
            columns_tpl = {
                'athena_datasource_arn': 'arn:aws:quicksight:us-east-1:123456789012:datasource/test',
                'athena_database_name': 'test_db',
                'athena_catalog_name': catalog_name,
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
                'primary_tag_name': 'team',
                'secondary_tag_name': 'project',
            }

            # Apply template substitution (same as safe_substitute in cid/common.py)
            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            compiled_dataset = json.loads(compiled_text)

            # Check all RelationalTable entries
            physical_table_map = compiled_dataset.get('PhysicalTableMap', {})
            for table_key, table_value in physical_table_map.items():
                if 'RelationalTable' in table_value:
                    rel_table = table_value['RelationalTable']
                    catalog_value = rel_table.get('Catalog')

                    assert catalog_value == catalog_name, (
                        f"In {relative_file}, RelationalTable '{table_key}' has "
                        f"Catalog='{catalog_value}' but expected '{catalog_name}'. "
                        f"The Catalog field is {'missing' if catalog_value is None else 'hardcoded'}."
                    )


class TestBugCondition_YAMLDashboardsHardcodedCatalog:
    """Test that YAML dashboard definitions use template variable for Catalog field.

    This test loads YAML dashboard definitions and checks that RelationalTable
    Catalog fields use ${athena_catalog_name} rather than hardcoded 'AwsDataCatalog'.
    """

    @given(catalog_name=catalog_name_strategy)
    @settings(max_examples=5, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_yaml_dashboards_use_catalog_variable(self, catalog_name):
        """
        **Validates: Requirements 1.1, 1.3**

        For any non-default catalog, after template substitution, all
        RelationalTable.Catalog fields in YAML dashboards MUST equal the selected catalog.
        """
        import glob
        yaml_files = glob.glob(os.path.join(DASHBOARDS_DIR, '**', '*.yaml'), recursive=True)
        assume(len(yaml_files) > 0)

        # Pick a representative sample - focus.yaml has RelationalTable entries
        sample_files = [f for f in yaml_files if 'focus' in f.lower()]
        if not sample_files:
            sample_files = yaml_files[:3]

        for yaml_file in sample_files:
            with open(yaml_file, 'r') as f:
                template_text = f.read()

            # Apply template substitution
            columns_tpl = {
                'athena_datasource_arn': 'arn:aws:quicksight:us-east-1:123456789012:datasource/test',
                'athena_database_name': 'test_db',
                'athena_catalog_name': catalog_name,
                'cur_database': 'cur_db',
                'cur_table_name': 'cur_table',
                'cur1_database': 'cur_db',
                'cur1_table_name': 'cur_table',
                'cur2_database': 'cur2_db',
                'cur2_table_name': 'cur2_table',
            }

            compiled_text = Template(template_text).safe_substitute(columns_tpl)
            resources = yaml.safe_load(compiled_text)

            # Navigate to datasets in the YAML structure
            datasets = resources.get('datasets', {})
            for dataset_name, dataset_def in datasets.items():
                dataset_data = dataset_def.get('data', {})
                if isinstance(dataset_data, str):
                    dataset_data = yaml.safe_load(dataset_data) or {}

                physical_table_map = dataset_data.get('PhysicalTableMap', {})
                for table_key, table_value in physical_table_map.items():
                    if 'RelationalTable' in table_value:
                        rel_table = table_value['RelationalTable']
                        catalog_value = rel_table.get('Catalog')
                        relative_path = os.path.relpath(yaml_file, PROJECT_ROOT)

                        assert catalog_value == catalog_name, (
                            f"In {relative_path}, dataset '{dataset_name}', "
                            f"RelationalTable '{table_key}' has Catalog='{catalog_value}' "
                            f"but expected '{catalog_name}'. "
                            f"The YAML dashboard has hardcoded 'AwsDataCatalog'."
                        )


class TestBugCondition_IAMPolicyHardcodedCatalog:
    """Test that IAM policy resource ARN uses the selected catalog.

    The IAM helper's ensure_data_source_role_exists method should accept a
    catalog_name parameter instead of hardcoding 'AwsDataCatalog'.
    """

    @given(catalog_name=catalog_name_strategy)
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_iam_policy_uses_selected_catalog_in_arn(self, catalog_name):
        """
        **Validates: Requirements 1.1**

        The IAM policy resource ARN must reference the selected catalog,
        not hardcoded 'AwsDataCatalog'.
        """
        # Read the IAM helper source to verify the hardcoded pattern is gone
        iam_file = os.path.join(PROJECT_ROOT, 'cid', 'helpers', 'iam.py')
        with open(iam_file, 'r') as f:
            iam_source = f.read()

        # The fix should have removed the hardcoded 'datacatalog/AwsDataCatalog'
        assert 'datacatalog/AwsDataCatalog' not in iam_source, (
            f"cid/helpers/iam.py contains hardcoded 'datacatalog/AwsDataCatalog' "
            f"in the IAM policy resource ARN. When catalog '{catalog_name}' is selected, "
            f"the ARN should reference 'datacatalog/{catalog_name}' instead."
        )


class TestBugCondition_ExportDoesNotTemplatizeCatalog:
    """Test that export.py templatizes the Catalog field in RelationalTable.

    This test reads the actual cid/export.py source to verify the fix is in place.
    """

    @given(catalog_name=catalog_name_strategy)
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_export_templatizes_catalog_field(self, catalog_name):
        """
        **Validates: Requirements 1.1**

        The export logic must templatize the Catalog field to ${athena_catalog_name}
        when processing RelationalTable entries.
        """
        # Read the actual export.py source code
        export_file = os.path.join(PROJECT_ROOT, 'cid', 'export.py')
        with open(export_file, 'r') as f:
            export_source = f.read()

        # Verify that the Catalog field is templatized in the RelationalTable block
        # The fix adds: value['RelationalTable']['Catalog'] = '${athena_catalog_name}'
        assert "${athena_catalog_name}" in export_source, (
            "cid/export.py does not contain '${athena_catalog_name}'. "
            "The export logic must templatize the Catalog field to '${athena_catalog_name}' "
            "when processing RelationalTable entries."
        )

        # Verify the specific pattern assigning to Catalog
        pattern = r"""value\['RelationalTable'\]\['Catalog'\]\s*=\s*['"]\$\{athena_catalog_name\}['"]"""
        assert re.search(pattern, export_source), (
            "cid/export.py does not assign '${athena_catalog_name}' to "
            "value['RelationalTable']['Catalog']. The export logic must templatize "
            "the Catalog field when processing RelationalTable entries."
        )
