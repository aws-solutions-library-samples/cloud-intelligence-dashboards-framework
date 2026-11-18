""" Unit tests for Athena.create_or_update_view drift handling (cid/helpers/athena.py)

An existing view is replaced with --on-drift override (with or without a
terminal), and is kept as is without a terminal otherwise, unless the caller
passes replace_if_no_tty (used for the CID-managed CUR proxy).
"""
from unittest.mock import MagicMock, patch

from cid.helpers.athena import Athena
from cid.helpers.cur_proxy import ProxyView
from cid.utils import IsolatedParameters, set_parameters


def _run(on_drift, terminal, replace_if_no_tty=False, diff=None):
    athena = Athena.__new__(Athena)
    athena._metadata = {'my_view': {}}
    athena.discover_views = MagicMock()
    athena.execute_query = MagicMock()
    athena.get_view_diff = MagicMock(return_value=diff)
    with IsolatedParameters(), patch('cid.helpers.athena.isatty', return_value=terminal), \
            patch('cid.helpers.athena.logger.trace', create=True):
        set_parameters({'on-drift': on_drift})
        athena.create_or_update_view('my_view', 'CREATE OR REPLACE VIEW my_view AS SELECT 1', replace_if_no_tty=replace_if_no_tty)
    return athena


def test_override_in_terminal_replaces_view():
    athena = _run('override', terminal=True)
    athena.get_view_diff.assert_not_called()
    athena.execute_query.assert_called_once()


def test_no_terminal_keeps_view():
    athena = _run('show', terminal=False)
    athena.execute_query.assert_not_called()


def test_no_terminal_override_replaces_view():
    """ explicit --on-drift override is honored in unattended runs (agents, CI, Lambda) """
    athena = _run('override', terminal=False)
    athena.get_view_diff.assert_not_called()
    athena.execute_query.assert_called_once()


def test_no_terminal_replaces_view_when_allowed():
    for on_drift in ('show', 'override'):
        athena = _run(on_drift, terminal=False, replace_if_no_tty=True)
        athena.get_view_diff.assert_not_called()
        athena.execute_query.assert_called_once()


def test_terminal_still_asks_when_replace_if_no_tty():
    diff = {'diff': True, 'printable': '+ new_column'}
    with patch('cid.helpers.athena.get_parameter', return_value='keep existing') as ask:
        athena = _run('show', terminal=True, replace_if_no_tty=True, diff=diff)
    athena.get_view_diff.assert_called_once()
    ask.assert_called_once()
    athena.execute_query.assert_not_called()


def test_proxy_update_replaces_without_terminal_and_logs_new_columns():
    proxy = ProxyView.__new__(ProxyView)
    proxy.name = 'cur1_proxy'
    proxy.target_cur_version = '1'
    proxy.exposed_fields = ['line_item_usage_account_id']
    proxy.fields_to_expose = ['line_item_usage_account_id', 'product_sku']
    proxy.fields_with_missing_requirements = []
    proxy.column_surely_exist = MagicMock(side_effect=lambda f: f != 'product_sku')
    proxy.read_from_athena = MagicMock()
    proxy.get_fields_from_sql = MagicMock(side_effect=lambda f: [f])
    proxy.get_sql_expression = MagicMock(side_effect=lambda f, t: f)
    proxy.source_column_equivalents = MagicMock(return_value=[])
    proxy.cur = MagicMock(database='db', table_name='cur2')
    proxy.cur.get_type_of_column.return_value = 'string'
    proxy.athena = MagicMock()
    with patch('cid.helpers.cur_proxy.logger') as log:
        proxy.create_or_update_view()
    _, kwargs = proxy.athena.create_or_update_view.call_args
    assert kwargs.get('replace_if_no_tty') is True
    assert any('product_sku' in str(c) for c in log.info.call_args_list)
    assert proxy.exposed_fields == ['line_item_usage_account_id', 'product_sku']
