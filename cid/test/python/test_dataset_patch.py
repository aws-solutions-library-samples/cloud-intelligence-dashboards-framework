""" Unit tests for Dataset.patch (cid/helpers/quicksight/dataset.py)

No AWS credentials or live connections required.
"""
import logging
from unittest.mock import MagicMock

from cid.logger import add_logging_level
from cid.helpers.quicksight.dataset import Dataset

add_logging_level('TRACE', logging.DEBUG - 5) # Dataset.patch uses logger.trace


def _new_experience_dataset():
    return {
        'DataSetId': 'summary_view',
        'Name': 'summary_view',
        'PhysicalTableMap': {
            'pt-summary': {'RelationalTable': {'Name': 'summary_view', 'Schema': 'cid_cur', 'InputColumns': [
                {'Name': 'linked_account_id', 'Type': 'STRING', 'Id': 'summary-view-linked-account-id'},
                {'Name': 'region', 'Type': 'STRING', 'Id': 'summary-view-region'},
            ]}},
            'pt-account-map': {'RelationalTable': {'Name': 'account_map', 'Schema': 'cid_cur', 'InputColumns': [
                {'Name': 'account_id', 'Type': 'STRING', 'Id': 'account-map-account-id'},
                {'Name': 'account_name', 'Type': 'STRING', 'Id': 'account-map-account-name'},
            ]}},
        },
        'DataPrepConfiguration': {'SourceTableMap': {}, 'TransformStepMap': {}, 'DestinationTableMap': {}},
    }


def _athena(columns_by_table):
    athena = MagicMock()
    athena.get_table_metadata.side_effect = lambda table, **_: {
        'Columns': [{'Name': name, 'Type': 'varchar'} for name in columns_by_table[table]]
    }
    return athena


def test_new_experience_skips_column_duplicated_in_joined_table():
    athena = _athena({
        'summary_view': ['linked_account_id', 'region'],
        'account_map': ['account_id', 'account_name', 'region', 'business_unit'],
    })
    result = Dataset.patch(_new_experience_dataset(), athena=athena)
    tables = result['PhysicalTableMap']
    summary_cols = [c['Name'] for c in tables['pt-summary']['RelationalTable']['InputColumns']]
    account_map_cols = [c['Name'] for c in tables['pt-account-map']['RelationalTable']['InputColumns']]
    assert summary_cols == ['linked_account_id', 'region']
    assert account_map_cols == ['account_id', 'account_name', 'business_unit']


def test_new_experience_skips_new_column_present_in_two_tables():
    athena = _athena({
        'summary_view': ['linked_account_id', 'region', 'cost_center'],
        'account_map': ['account_id', 'account_name', 'cost_center'],
    })
    result = Dataset.patch(_new_experience_dataset(), athena=athena)
    tables = result['PhysicalTableMap']
    all_names = [
        c['Name']
        for pt in tables.values()
        for c in pt['RelationalTable']['InputColumns']
    ]
    assert all_names.count('cost_center') == 1
