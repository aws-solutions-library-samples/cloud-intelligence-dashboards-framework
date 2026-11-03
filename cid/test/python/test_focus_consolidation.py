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
