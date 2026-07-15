"""Unit tests for cross-dimension tag merging.

All tests import and exercise the *real* production classes:
- ``AbstractCUR.tag_fields_by_dimension``  (cid/helpers/cur.py)
- ``Cid._tag_to_name``                     (cid/common.py)
- ``Cid._selector_dimension``              (cid/common.py)
- ``Cid._build_merge_sql``                 (cid/common.py)
- ``Cid._build_tag_choices``               (cid/common.py)
- ``Cid._build_tags_json_sql``             (cid/common.py)

No AWS credentials or live Athena connections are required; both classes are
instantiated via lightweight stubs that only satisfy the attributes exercised
by the code under test.
"""
import pytest


# ---------------------------------------------------------------------------
# Stub: minimal AbstractCUR subclass (no Athena/Glue required)
# ---------------------------------------------------------------------------

def _make_cur(selectors, version='2'):
    """Return a real ``AbstractCUR`` subclass with ``tag_and_cost_category_fields``
    pre-seeded from *selectors* and ``version`` fixed to *version*.
    """
    from cid.helpers.cur import AbstractCUR

    class StubCUR(AbstractCUR):
        def ensure_column(self, column, column_type=None):
            pass

        @property
        def metadata(self):
            cols = [{'Name': 'bill_payer_account_name'}] if version == '2' else []
            return {'Columns': cols, 'PartitionKeys': [], 'Name': 'stub_table'}

    stub = StubCUR.__new__(StubCUR)
    stub.athena = None
    stub.glue = None
    stub.proxy = None
    stub._metadata = None
    stub._tag_and_cost_category = list(selectors)
    return stub


# ---------------------------------------------------------------------------
# AbstractCUR.tag_fields_by_dimension
# ---------------------------------------------------------------------------

class TestTagFieldsByDimension:

    def test_no_cross_dimension_returns_empty(self):
        cur = _make_cur([
            "resource_tags['user_owner']",
            "tags['accountTag/cost_center']",
        ])
        assert cur.tag_fields_by_dimension == {}

    def test_resource_and_account_same_key(self):
        cur = _make_cur([
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
        ])
        result = cur.tag_fields_by_dimension
        assert 'environment' in result
        assert len(result['environment']) == 2
        assert "resource_tags['user_environment']" in result['environment']
        assert "tags['accountTag/environment']" in result['environment']

    def test_three_way_cross_dimension(self):
        cur = _make_cur([
            "resource_tags['user_team']",
            "tags['accountTag/team']",
            "tags['iamPrincipal/team']",
        ])
        result = cur.tag_fields_by_dimension
        assert 'team' in result
        assert len(result['team']) == 3

    def test_normalises_special_chars_in_key(self):
        cur = _make_cur([
            "resource_tags['user_my-tag']",
            "tags['accountTag/my-tag']",
        ])
        result = cur.tag_fields_by_dimension
        assert 'my_tag' in result

    def test_only_returns_multi_dimension_keys(self):
        cur = _make_cur([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
            "resource_tags['user_owner']",
        ])
        result = cur.tag_fields_by_dimension
        assert 'env' in result
        assert 'owner' not in result

    def test_cur1_returns_empty_dict(self):
        cur = _make_cur([], version='1')
        assert cur.tag_fields_by_dimension == {}

    def test_line_item_iam_principal_bare_key(self):
        cur = _make_cur([
            'line_item_iam_principal',
            "tags['iamPrincipal/iam_principal']",
        ])
        result = cur.tag_fields_by_dimension
        assert 'iam_principal' in result
        assert len(result['iam_principal']) == 2

    def test_cost_category_case_sensitive_no_merge(self):
        cur = _make_cur([
            "resource_tags['user_billing']",
            "cost_category['Billing']",
        ])
        assert cur.tag_fields_by_dimension == {}


# ---------------------------------------------------------------------------
# Cid._tag_to_name
# ---------------------------------------------------------------------------

class TestTagToName:

    @pytest.fixture(autouse=True)
    def _import(self):
        from cid.common import Cid
        self.fn = Cid._tag_to_name

    def test_resource_tag_cur2(self):
        assert self.fn("resource_tags['user_environment']") == 'tag_environment'

    def test_resource_tag_with_hyphen(self):
        assert self.fn("resource_tags['user_my-tag']") == 'tag_my_tag'

    def test_resource_tag_cur1_flat_column(self):
        # CUR1 flat columns keep 'user_' in the name after stripping 'resource_tags_',
        # because the 'user_' replacement only targets MAP accessor syntax ("'user_").
        assert self.fn('resource_tags_user_environment') == 'tag_user_environment'

    def test_account_tag(self):
        assert self.fn("tags['accountTag/CostCenter']") == 'account_tag_CostCenter'

    def test_iam_principal_tag(self):
        assert self.fn("tags['iamPrincipal/Team']") == 'iam_principal_tag_Team'

    def test_user_attribute_tag(self):
        assert self.fn("tags['userAttribute/Department']") == 'user_attribute_tag_Department'

    def test_cost_category_cur2(self):
        assert self.fn("cost_category['BillingTeam']") == 'cost_category_BillingTeam'

    def test_cost_category_cur1_flat_column(self):
        assert self.fn('cost_category_BillingTeam') == 'cost_category_BillingTeam'

    def test_line_item_iam_principal(self):
        assert self.fn('line_item_iam_principal') == 'iam_principal'

    def test_aws_reserved_prefix_escaped(self):
        # 'aws_' prefix in resource tag key gets escaped to 'tag_aws_'
        result = self.fn("resource_tags['user_aws_something']")
        assert 'aws' in result

    def test_non_word_chars_replaced_with_underscore(self):
        result = self.fn("resource_tags['user_my.tag:name']")
        assert re.match(r'^\w+$', result), f"Expected only word chars, got: {result}"


# ---------------------------------------------------------------------------
# Cid._selector_dimension
# ---------------------------------------------------------------------------

class TestSelectorDimension:

    @pytest.fixture(autouse=True)
    def _import(self):
        from cid.common import Cid
        self.fn = Cid._selector_dimension

    def test_resource_tags(self):
        assert self.fn("resource_tags['user_env']") == 'resource_tags'

    def test_account_tag(self):
        assert self.fn("tags['accountTag/env']") == 'tags_account'

    def test_iam_principal_tag(self):
        assert self.fn("tags['iamPrincipal/team']") == 'tags_iam_principal'

    def test_user_attribute_tag(self):
        assert self.fn("tags['userAttribute/dept']") == 'tags_user_attribute'

    def test_cost_category(self):
        assert self.fn("cost_category['BillingTeam']") == 'cost_category'

    def test_line_item_iam_principal(self):
        assert self.fn('line_item_iam_principal') == 'tags_iam_principal'


# ---------------------------------------------------------------------------
# Cid._build_merge_sql
# ---------------------------------------------------------------------------

class TestBuildMergeSql:

    SELECTORS = [
        "resource_tags['user_environment']",
        "tags['accountTag/environment']",
        "tags['iamPrincipal/environment']",
    ]

    @pytest.fixture(autouse=True)
    def _import(self):
        from cid.common import Cid
        self.fn = Cid._build_merge_sql

    def test_resource_first_order(self):
        sql = self.fn('environment', self.SELECTORS, 'resource_first')
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account  = sql.index("tags['accountTag/environment']")
        assert pos_resource < pos_account

    def test_account_first_order(self):
        sql = self.fn('environment', self.SELECTORS, 'account_first')
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account  = sql.index("tags['accountTag/environment']")
        assert pos_account < pos_resource

    def test_coalesce_wraps_all_selectors(self):
        sql = self.fn('environment', self.SELECTORS, 'resource_first')
        assert 'COALESCE' in sql
        for sel in self.SELECTORS:
            assert f"NULLIF({sel}, '')" in sql

    def test_nullif_uses_empty_string_sentinel(self):
        sql = self.fn('env', self.SELECTORS[:2], 'resource_first')
        assert 'NULLIF(' in sql
        assert ", '')" in sql

    def test_unknown_strategy_falls_back_to_resource_first(self):
        sql_known   = self.fn('k', self.SELECTORS, 'resource_first')
        sql_unknown = self.fn('k', self.SELECTORS, 'nonexistent_strategy')
        assert sql_known == sql_unknown

    def test_two_selectors(self):
        selectors = ["resource_tags['user_env']", "tags['accountTag/env']"]
        sql = self.fn('env', selectors, 'resource_first')
        assert "NULLIF(resource_tags['user_env'], '')" in sql
        assert "NULLIF(tags['accountTag/env'], '')" in sql

    def test_single_selector_still_wraps_in_coalesce(self):
        sql = self.fn('env', self.SELECTORS[:1], 'resource_first')
        assert 'COALESCE' in sql
        assert "NULLIF(resource_tags['user_environment'], '')" in sql


# ---------------------------------------------------------------------------
# Cid._build_tag_choices
# ---------------------------------------------------------------------------

class TestBuildTagChoices:

    @pytest.fixture(autouse=True)
    def _import(self):
        from cid.common import Cid
        self.fn = Cid._build_tag_choices

    def _tags_and_names(self):
        from cid.common import Cid
        selectors = [
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
            "resource_tags['user_owner']",
        ]
        return {Cid._tag_to_name(s): s for s in selectors}

    def test_no_merge_all_passthrough(self):
        tags_and_names = self._tags_and_names()
        merged = self.fn(tags_and_names, set(), {})
        assert merged == {}

    def test_merged_key_keeps_individual_entries(self):
        tags_and_names = self._tags_and_names()
        merge_map = {
            'environment': [
                "resource_tags['user_environment']",
                "tags['accountTag/environment']",
            ]
        }
        merged = self.fn(tags_and_names, {'environment'}, merge_map)
        assert 'merged_environment' in merged

    def test_non_merged_key_stays_in_passthrough(self):
        tags_and_names = self._tags_and_names()
        merge_map = {
            'environment': [
                "resource_tags['user_environment']",
                "tags['accountTag/environment']",
            ]
        }
        merged = self.fn(tags_and_names, {'environment'}, merge_map)
        assert 'merged_environment' in merged

    def test_merged_display_value_contains_bare_key_and_selectors(self):
        tags_and_names = self._tags_and_names()
        merge_map = {
            'environment': [
                "resource_tags['user_environment']",
                "tags['accountTag/environment']",
            ]
        }
        merged = self.fn(tags_and_names, {'environment'}, merge_map)
        bare_key, selectors = merged['merged_environment']
        assert bare_key == 'environment'
        assert len(selectors) == 2


# ---------------------------------------------------------------------------
# Cid._build_tags_json_sql
# ---------------------------------------------------------------------------

import re

class TestBuildTagsJsonSql:

    @pytest.fixture(autouse=True)
    def _import(self):
        from cid.common import Cid
        self.Cid = Cid
        self.obj = Cid.__new__(Cid)  # no __init__ needed; method only uses self for _build_merge_sql

    def _simple_tags_and_names(self):
        return {
            'tag_environment': "resource_tags['user_environment']",
            'tag_owner':       "resource_tags['user_owner']",
        }

    def test_returns_empty_json_for_empty_selection(self):
        result = self.obj._build_tags_json_sql([], {}, {}, 'resource_first')
        assert result == "'{}'"

    def test_plain_tag_rendered_correctly(self):
        tags_and_names = self._simple_tags_and_names()
        result = self.obj._build_tags_json_sql(
            ['tag_environment'], {}, tags_and_names, 'resource_first'
        )
        assert "('tag_environment', resource_tags['user_environment'])" in result
        assert 'json_format' in result
        assert 'MAP_FROM_ENTRIES' in result

    def test_merged_tag_uses_coalesce(self):
        tags_and_names = self._simple_tags_and_names()
        merge_map_selectors = [
            "resource_tags['user_environment']",
            "tags['accountTag/environment']",
        ]
        merged_display = {'merged_environment': ('environment', merge_map_selectors)}
        result = self.obj._build_tags_json_sql(
            ['merged_environment'], merged_display, tags_and_names, 'resource_first'
        )
        assert 'COALESCE' in result
        assert "NULLIF(resource_tags['user_environment'], '')" in result

    def test_unknown_tag_name_skipped(self):
        result = self.obj._build_tags_json_sql(
            ['nonexistent_tag'], {}, {}, 'resource_first'
        )
        assert result == "'{}'"

    def test_special_chars_in_name_sanitised(self):
        tags_and_names = {'tag_my_tag': "resource_tags['user_my-tag']"}
        result = self.obj._build_tags_json_sql(
            ['tag_my_tag'], {}, tags_and_names, 'resource_first'
        )
        assert "'tag_my_tag'" in result
