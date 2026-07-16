"""Unit tests for cross-dimension tag merging (cid/helpers/tag_merger.py).

Tests cover:
- normalise_key(): bare key extraction from all CUR2 selector formats
- selector_dimension(): dimension classification
- TagMerger.merge_candidates: grouping and filtering logic
- TagMerger.build_merge_expression(): COALESCE SQL generation
- TagMerger.inject_merged_entries(): integration with tags_and_names dict

No AWS credentials or live Athena connections required.
"""
import sys
import os
import importlib.util
import pytest
from unittest.mock import MagicMock

# --- Bootstrap: import tag_merger.py without triggering the full cid package ---
# The cid.helpers.__init__.py imports boto3 which fails on older OpenSSL.
# We mock cid.utils and import tag_merger directly via importlib.

_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)
))))

# Create a mock cid.utils with the functions tag_merger imports
_mock_utils = MagicMock()
_mock_utils.get_parameters = MagicMock(return_value={})
_mock_utils.get_parameter = MagicMock(return_value=[])
_mock_utils.isatty = MagicMock(return_value=False)
_mock_utils.set_parameters = MagicMock()
_mock_utils.cid_print = MagicMock()
_mock_utils.get_yesno_parameter = MagicMock(return_value=True)

# Inject mock into sys.modules before importing tag_merger
sys.modules['cid'] = MagicMock()
sys.modules['cid.utils'] = _mock_utils

# Import tag_merger.py directly
_tag_merger_path = os.path.join(_repo_root, 'cid', 'helpers', 'tag_merger.py')
_spec = importlib.util.spec_from_file_location('cid.helpers.tag_merger', _tag_merger_path)
_tag_merger_mod = importlib.util.module_from_spec(_spec)
sys.modules['cid.helpers.tag_merger'] = _tag_merger_mod
_spec.loader.exec_module(_tag_merger_mod)

# Pull out the public API
normalise_key = _tag_merger_mod.normalise_key
selector_dimension = _tag_merger_mod.selector_dimension
TagMerger = _tag_merger_mod.TagMerger
STRATEGIES = _tag_merger_mod.STRATEGIES


# ---------------------------------------------------------------------------
# normalise_key()
# ---------------------------------------------------------------------------

class TestNormaliseKey:

    def test_resource_tag_cur2(self):
        assert normalise_key("resource_tags['user_environment']") == 'environment'

    def test_resource_tag_with_hyphen(self):
        assert normalise_key("resource_tags['user_my-tag']") == 'my_tag'

    def test_resource_tag_aws_prefix(self):
        # aws_ prefixed tags keep 'aws' after stripping user_
        assert normalise_key("resource_tags['user_aws_something']") == 'aws_something'

    def test_account_tag_simple(self):
        assert normalise_key("tags['accountTag/environment']") == 'environment'

    def test_account_tag_camel_case(self):
        assert normalise_key("tags['accountTag/BusinessUnit']") == 'business_unit'

    def test_account_tag_pascal_case(self):
        assert normalise_key("tags['accountTag/CostCenter']") == 'cost_center'

    def test_iam_principal_tag(self):
        assert normalise_key("tags['iamPrincipal/Team']") == 'team'

    def test_iam_principal_tag_camel(self):
        assert normalise_key("tags['iamPrincipal/CostCenter']") == 'cost_center'

    def test_user_attribute_tag(self):
        assert normalise_key("tags['userAttribute/Department']") == 'department'

    def test_cost_category_cur2(self):
        assert normalise_key("cost_category['BillingTeam']") == 'billing_team'

    def test_cost_category_lowercase(self):
        assert normalise_key("cost_category['billing']") == 'billing'

    def test_line_item_iam_principal(self):
        assert normalise_key('line_item_iam_principal') == 'iam_principal'

    def test_cur1_flat_resource_tag(self):
        assert normalise_key('resource_tags_user_environment') == 'environment'

    def test_cur1_flat_cost_category(self):
        assert normalise_key('cost_category_BillingTeam') == 'billing_team'

    def test_special_chars_normalised(self):
        result = normalise_key("resource_tags['user_my.tag:name']")
        assert '.' not in result
        assert ':' not in result
        assert result == 'my_tag_name'

    def test_all_lowercase(self):
        result = normalise_key("tags['accountTag/ENVIRONMENT']")
        assert result == 'environment'

    def test_consecutive_uppercase(self):
        # AWS → aws (no underscore between consecutive uppercase)
        result = normalise_key("tags['accountTag/AWSService']")
        assert result == 'awsservice'


# ---------------------------------------------------------------------------
# selector_dimension()
# ---------------------------------------------------------------------------

class TestSelectorDimension:

    def test_resource_tags(self):
        assert selector_dimension("resource_tags['user_env']") == 'resource_tags'

    def test_account_tag(self):
        assert selector_dimension("tags['accountTag/env']") == 'account_tags'

    def test_iam_principal_tag(self):
        assert selector_dimension("tags['iamPrincipal/team']") == 'iam_principal'

    def test_user_attribute_tag(self):
        assert selector_dimension("tags['userAttribute/dept']") == 'user_attribute'

    def test_cost_category(self):
        assert selector_dimension("cost_category['BillingTeam']") == 'cost_category'

    def test_line_item_iam_principal(self):
        assert selector_dimension('line_item_iam_principal') == 'iam_principal'

    def test_cur1_resource_tags(self):
        assert selector_dimension('resource_tags_user_env') == 'resource_tags'

    def test_cur1_cost_category(self):
        assert selector_dimension('cost_category_billing') == 'cost_category'


# ---------------------------------------------------------------------------
# TagMerger.merge_candidates
# ---------------------------------------------------------------------------

class TestMergeCandidates:

    def test_no_cross_dimension_returns_empty(self):
        merger = TagMerger([
            "resource_tags['user_owner']",
            "tags['accountTag/cost_center']",
        ])
        assert merger.merge_candidates == {}

    def test_same_key_resource_and_account(self):
        merger = TagMerger([
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
        ])
        assert 'environment' in merger.merge_candidates
        assert len(merger.merge_candidates['environment']) == 2

    def test_three_way_cross_dimension(self):
        merger = TagMerger([
            "resource_tags['user_team']",
            "tags['accountTag/team']",
            "tags['iamPrincipal/team']",
        ])
        assert 'team' in merger.merge_candidates
        assert len(merger.merge_candidates['team']) == 3

    def test_only_multi_dimension_returned(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
            "resource_tags['user_owner']",  # single dimension
        ])
        assert 'env' in merger.merge_candidates
        assert 'owner' not in merger.merge_candidates

    def test_case_insensitive_matching(self):
        """CamelCase account tags match lowercase resource tags."""
        merger = TagMerger([
            "resource_tags['user_business_unit']",
            "tags['accountTag/BusinessUnit']",
        ])
        assert 'business_unit' in merger.merge_candidates

    def test_cost_category_different_case_no_merge(self):
        """cost_category['Billing'] and resource_tags['user_billing'] should merge
        because normalised keys are both 'billing'."""
        merger = TagMerger([
            "resource_tags['user_billing']",
            "cost_category['Billing']",
        ])
        assert 'billing' in merger.merge_candidates

    def test_empty_selectors(self):
        merger = TagMerger([])
        assert merger.merge_candidates == {}

    def test_duplicate_dimension_not_merged(self):
        """Two resource tags with same bare key should NOT be treated as merge candidates."""
        merger = TagMerger([
            "resource_tags['user_env']",
            "resource_tags['user_env']",  # duplicate
        ])
        assert merger.merge_candidates == {}

    def test_caching(self):
        """merge_candidates should be computed once and cached."""
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        result1 = merger.merge_candidates
        result2 = merger.merge_candidates
        assert result1 is result2


# ---------------------------------------------------------------------------
# TagMerger.build_merge_expression()
# ---------------------------------------------------------------------------

class TestBuildMergeExpression:

    def setup_method(self):
        self.merger = TagMerger([
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
            "tags['iamPrincipal/environment']",
        ])

    def test_resource_first_order(self):
        sql = self.merger.build_merge_expression('environment', 'resource_first')
        # resource_tags should come before accountTag
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account = sql.index("tags['accountTag/environment']")
        assert pos_resource < pos_account

    def test_account_first_order(self):
        sql = self.merger.build_merge_expression('environment', 'account_first')
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account = sql.index("tags['accountTag/environment']")
        assert pos_account < pos_resource

    def test_coalesce_wraps_all(self):
        sql = self.merger.build_merge_expression('environment', 'resource_first')
        assert sql.startswith('COALESCE(')
        assert sql.endswith(')')
        assert "NULLIF(resource_tags['user_environment'], '')" in sql
        assert "NULLIF(tags['accountTag/environment'], '')" in sql
        assert "NULLIF(tags['iamPrincipal/environment'], '')" in sql

    def test_nullif_empty_string_sentinel(self):
        sql = self.merger.build_merge_expression('environment', 'resource_first')
        assert "NULLIF(" in sql
        assert ", '')" in sql

    def test_unknown_strategy_falls_back(self):
        sql_known = self.merger.build_merge_expression('environment', 'resource_first')
        sql_unknown = self.merger.build_merge_expression('environment', 'nonexistent')
        assert sql_known == sql_unknown

    def test_two_selectors(self):
        merger = TagMerger([
            "resource_tags['user_app']",
            "tags['accountTag/app']",
        ])
        sql = merger.build_merge_expression('app', 'resource_first')
        assert "NULLIF(resource_tags['user_app'], '')" in sql
        assert "NULLIF(tags['accountTag/app'], '')" in sql

    def test_invalid_key_raises(self):
        with pytest.raises(ValueError, match='No merge candidates'):
            self.merger.build_merge_expression('nonexistent', 'resource_first')


# ---------------------------------------------------------------------------
# TagMerger.inject_merged_entries()
# ---------------------------------------------------------------------------

class TestInjectMergedEntries:

    def test_adds_merged_key(self):
        merger = TagMerger([
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
        ])
        tags_and_names = {
            'tag_environment': "resource_tags['user_environment']",
            'account_tag_environment': "tags['accountTag/environment']",
            'tag_owner': "resource_tags['user_owner']",
        }
        result = merger.inject_merged_entries(
            tags_and_names, {'environment'}, 'resource_first'
        )
        # Merged entry added
        assert 'environment' in result
        assert 'COALESCE' in result['environment']
        # Originals preserved
        assert 'tag_environment' in result
        assert 'account_tag_environment' in result
        assert 'tag_owner' in result

    def test_does_not_mutate_original(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        original = {'tag_env': "resource_tags['user_env']"}
        result = merger.inject_merged_entries(original, {'env'}, 'resource_first')
        assert 'env' in result
        assert 'env' not in original  # original unchanged

    def test_empty_keys_to_merge(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        tags_and_names = {'tag_env': "resource_tags['user_env']"}
        result = merger.inject_merged_entries(tags_and_names, set(), 'resource_first')
        assert result == tags_and_names

    def test_key_not_in_candidates_skipped(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        tags_and_names = {'tag_env': "resource_tags['user_env']"}
        result = merger.inject_merged_entries(
            tags_and_names, {'nonexistent'}, 'resource_first'
        )
        assert 'nonexistent' not in result


# ---------------------------------------------------------------------------
# TagMerger.resolve_merge_keys() — parameter persistence
# ---------------------------------------------------------------------------

class TestResolveMergeKeys:

    def test_non_interactive_returns_empty(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        _mock_utils.isatty.return_value = False
        _mock_utils.get_parameters.return_value = {}
        keys, strategy = merger.resolve_merge_keys()
        assert keys == set()
        assert strategy == 'resource_first'

    def test_cached_keys_returned(self):
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        _mock_utils.get_parameters.return_value = {
            'resource-tags-merge-tags': 'env',
            'resource-tags-merge-strategy': 'account_first',
        }
        keys, strategy = merger.resolve_merge_keys()
        assert keys == {'env'}
        assert strategy == 'account_first'

    def test_cached_stale_keys_pruned(self):
        """Keys no longer in current CUR are removed from cached selection."""
        merger = TagMerger([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ])
        _mock_utils.get_parameters.return_value = {
            'resource-tags-merge-tags': 'env,stale_key',
            'resource-tags-merge-strategy': 'resource_first',
        }
        keys, strategy = merger.resolve_merge_keys()
        assert keys == {'env'}
        assert 'stale_key' not in keys

    def test_no_candidates_returns_empty(self):
        merger = TagMerger([
            "resource_tags['user_owner']",
        ])
        _mock_utils.isatty.return_value = True
        _mock_utils.get_parameters.return_value = {}
        keys, strategy = merger.resolve_merge_keys()
        assert keys == set()
