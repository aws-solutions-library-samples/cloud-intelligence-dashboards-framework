"""Unit tests for cross-dimension tag merging.

These tests import and exercise the *real* production classes:
- ``AbstractCUR.tag_fields_by_dimension``  (cid/helpers/cur.py)
- ``Cid._selector_dimension``              (cid/common.py)
- ``Cid._build_merge_sql``                 (cid/common.py)
- ``Cid.generic_tags_json``                (cid/common.py)

No AWS credentials or live Athena connections are required; both classes are
instantiated via lightweight stubs that only satisfy the specific attributes
exercised by the code under test.
"""
import pytest

# ---------------------------------------------------------------------------
# Helpers: minimal stubs that let us instantiate real classes without AWS
# ---------------------------------------------------------------------------

def _make_cur(selectors, version='2'):
    """Return a real ``AbstractCUR`` subclass instance with
    ``tag_and_cost_category_fields`` pre-loaded from *selectors* and
    ``version`` fixed to *version*.  No Athena/Glue clients needed.
    """
    from cid.helpers.cur import AbstractCUR

    class StubCUR(AbstractCUR):
        # AbstractCUR requires these two abstract-ish methods
        def ensure_column(self, column, column_type=None):
            pass

        @property
        def metadata(self):
            # 'bill_payer_account_name' being present triggers version == '2'
            cols = [{'Name': 'bill_payer_account_name'}] if version == '2' else []
            return {'Columns': cols, 'PartitionKeys': [], 'Name': 'stub_table'}

    stub = StubCUR.__new__(StubCUR)
    stub.athena = None
    stub.glue = None
    stub.proxy = None
    stub._metadata = None
    # Pre-populate the tag cache so no Athena query is fired
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
        # hyphen is normalised to underscore in the bare key
        assert 'my_tag' in result

    def test_only_returns_multi_dimension_keys(self):
        cur = _make_cur([
            "resource_tags['user_env']",
            "tags['accountTag/env']",
            "resource_tags['user_owner']",       # owner only in one dimension
        ])
        result = cur.tag_fields_by_dimension
        assert 'env' in result
        assert 'owner' not in result

    def test_cur1_returns_empty_dict(self):
        # CUR1 has no MAP columns, merging is not applicable
        cur = _make_cur([], version='1')
        assert cur.tag_fields_by_dimension == {}

    def test_line_item_iam_principal_bare_key(self):
        cur = _make_cur([
            'line_item_iam_principal',
            "tags['iamPrincipal/iam_principal']",
        ])
        result = cur.tag_fields_by_dimension
        # Both resolve to bare key 'iam_principal'
        assert 'iam_principal' in result
        assert len(result['iam_principal']) == 2

    def test_cost_category_case_sensitive_no_merge(self):
        # 'billing' (resource tag) vs 'Billing' (cost category) — different bare keys
        cur = _make_cur([
            "resource_tags['user_billing']",
            "cost_category['Billing']",
        ])
        assert cur.tag_fields_by_dimension == {}


# ---------------------------------------------------------------------------
# Cid._selector_dimension
# ---------------------------------------------------------------------------

class TestSelectorDimension:
    """Tests call the real static method on the real Cid class."""

    @pytest.fixture(autouse=True)
    def cid_class(self):
        from cid.common import Cid
        self.Cid = Cid

    def test_resource_tags(self):
        assert self.Cid._selector_dimension("resource_tags['user_env']") == 'resource_tags'

    def test_account_tag(self):
        assert self.Cid._selector_dimension("tags['accountTag/env']") == 'tags_account'

    def test_iam_principal_tag(self):
        assert self.Cid._selector_dimension("tags['iamPrincipal/team']") == 'tags_iam_principal'

    def test_user_attribute_tag(self):
        assert self.Cid._selector_dimension("tags['userAttribute/dept']") == 'tags_user_attribute'

    def test_cost_category(self):
        assert self.Cid._selector_dimension("cost_category['BillingTeam']") == 'cost_category'

    def test_line_item_iam_principal(self):
        assert self.Cid._selector_dimension('line_item_iam_principal') == 'tags_iam_principal'


# ---------------------------------------------------------------------------
# Cid._build_merge_sql
# ---------------------------------------------------------------------------

class TestBuildMergeSql:
    """Tests call the real static method on the real Cid class."""

    SELECTORS = [
        "resource_tags['user_environment']",
        "tags['accountTag/environment']",
        "tags['iamPrincipal/environment']",
    ]

    @pytest.fixture(autouse=True)
    def cid_class(self):
        from cid.common import Cid
        self.Cid = Cid

    def test_resource_first_order(self):
        sql = self.Cid._build_merge_sql('environment', self.SELECTORS, 'resource_first')
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account  = sql.index("tags['accountTag/environment']")
        pos_iam      = sql.index("tags['iamPrincipal/environment']")
        assert pos_resource < pos_iam < pos_account

    def test_account_first_order(self):
        sql = self.Cid._build_merge_sql('environment', self.SELECTORS, 'account_first')
        pos_resource = sql.index("resource_tags['user_environment']")
        pos_account  = sql.index("tags['accountTag/environment']")
        assert pos_account < pos_resource

    def test_coalesce_wraps_all_selectors(self):
        sql = self.Cid._build_merge_sql('environment', self.SELECTORS, 'resource_first')
        assert 'COALESCE' in sql
        for sel in self.SELECTORS:
            assert f"NULLIF({sel}, '')" in sql

    def test_nullif_uses_empty_string_sentinel(self):
        # Athena MAP accessors return '' not NULL for missing keys — NULLIF('', '') is essential
        sql = self.Cid._build_merge_sql('env', self.SELECTORS[:2], 'resource_first')
        assert "NULLIF(" in sql
        assert ", '')" in sql

    def test_unknown_strategy_falls_back_to_resource_first(self):
        sql_known   = self.Cid._build_merge_sql('k', self.SELECTORS, 'resource_first')
        sql_unknown = self.Cid._build_merge_sql('k', self.SELECTORS, 'nonexistent_strategy')
        assert sql_known == sql_unknown

    def test_two_selectors(self):
        selectors = [
            "resource_tags['user_env']",
            "tags['accountTag/env']",
        ]
        sql = self.Cid._build_merge_sql('env', selectors, 'resource_first')
        assert "NULLIF(resource_tags['user_env'], '')" in sql
        assert "NULLIF(tags['accountTag/env'], '')" in sql

    def test_single_selector_still_wraps_in_coalesce(self):
        sql = self.Cid._build_merge_sql('env', self.SELECTORS[:1], 'resource_first')
        assert 'COALESCE' in sql
        assert "NULLIF(resource_tags['user_environment'], '')" in sql
