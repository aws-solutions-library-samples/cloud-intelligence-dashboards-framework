""" Unit tests for FOCUS consolidation view (cid/helpers/focus_consolidation.py)

GCP FOCUS export has no ServiceCategory and CommitmentDiscount* columns.
It must still be discovered as a FOCUS table.
"""
from cid.helpers.focus_consolidation import FOCUS_MINIMUM_COLUMNS

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
