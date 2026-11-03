""" Unit tests for FOCUS consolidation view (cid/helpers/focus_consolidation.py)

GCP FOCUS export has no ServiceCategory and CommitmentDiscount* columns.
It must still be discovered as a FOCUS table, and its tags must be built from x_tags.
"""
from cid.helpers.focus_consolidation import FOCUS_MINIMUM_COLUMNS, FocusConsolidationView

# Columns of a GCP FOCUS export table in Athena (provider-specific x_* columns omitted)
GCP_COLUMNS = [
    'availabilityzone', 'billedcost', 'billingcurrency', 'billingperiodstart', 'billingperiodend',
    'chargecategory', 'chargeclass', 'chargedescription', 'chargeperiodstart', 'chargeperiodend',
    'consumedquantity', 'consumedunit', 'contractedcost', 'contractedunitprice', 'listcost',
    'listunitprice', 'pricingcategory', 'pricingquantity', 'pricingunit', 'providername',
    'publishername', 'regionid', 'regionname', 'resourceid', 'resourcename', 'servicename',
    'skuid', 'skupriceid', 'subaccountid', 'subaccountname', 'effectivecost', 'pricingcurrency',
    'pricingcurrencycontractedunitprice', 'pricingcurrencyeffectivecost',
    'pricingcurrencylistunitprice', 'billingaccounttype', 'billingaccountid',
]


def test_gcp_table_has_minimum_columns():
    assert set(FOCUS_MINIMUM_COLUMNS) <= set(GCP_COLUMNS)


# Glue types of GCP key/value columns as registered by the GCP CID Glue job
X_TAGS = 'array<struct<Key:string,Value:string,x_Inherited:boolean,x_Namespace:string>>'
X_LABELS = 'array<struct<Key:string,Value:string>>'
COLUMNS = {'tags': 'map<string,string>'}


def _tags_expr(source_columns):
    view = FocusConsolidationView(athena=None, columns=COLUMNS)
    return view._column_expression('tags', COLUMNS['tags'], source_columns, [])


def test_tags_column_is_used_when_present():
    assert _tags_expr({'tags': 'map<string,string>', 'x_tags': X_TAGS}) == 'tags'


def test_tags_null_when_no_source():
    assert _tags_expr({'servicename': 'string'}) == 'CAST(NULL AS MAP<VARCHAR,VARCHAR>) tags'


def test_tags_from_gcp_x_tags():
    expr = _tags_expr({'x_tags': X_TAGS, 'x_labels': X_LABELS})
    assert expr == (
        'COALESCE(transform_values(multimap_from_entries(transform('
        'filter(x_tags, x -> x.key IS NOT NULL), x -> ROW(x.key, x.value))), '
        '(k, v) -> element_at(v, 1)), MAP()) tags'
    )
    assert 'x_labels' not in expr


def test_tags_ignores_non_key_value_x_tags():
    assert _tags_expr({'x_tags': 'string'}) == 'CAST(NULL AS MAP<VARCHAR,VARCHAR>) tags'


def test_partition_key_counts_as_column():
    """ GCP FOCUS export table has billingaccountid as a partition key, not a column """
    from unittest.mock import MagicMock
    from cid.helpers.athena import Athena
    table = {
        'Name': 'gcp_focus_export',
        'TableType': 'EXTERNAL_TABLE',
        'Columns': [{'Name': col} for col in GCP_COLUMNS if col != 'billingaccountid'],
        'PartitionKeys': [{'Name': 'billingaccountid'}, {'Name': 'export_year'}],
    }
    athena = Athena.__new__(Athena)
    client = MagicMock()
    client.get_paginator.return_value.paginate.return_value.search.return_value = iter([table])
    athena._client = client
    found = list(athena.find_tables_with_columns(FOCUS_MINIMUM_COLUMNS, database_name='gcpapp_db', catalog_name='AwsDataCatalog'))
    assert [t['Name'] for t in found] == ['gcp_focus_export']
