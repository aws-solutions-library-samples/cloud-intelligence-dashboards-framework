""" Unit tests for Athena.create_or_update_view drift handling (cid/helpers/athena.py)

An existing view is replaced with --on-drift override in a terminal, and is
kept as is without a terminal.
"""
from unittest.mock import MagicMock, patch

from cid.helpers.athena import Athena
from cid.utils import IsolatedParameters, set_parameters


def _run(on_drift, terminal):
    athena = Athena.__new__(Athena)
    athena._metadata = {'my_view': {}}
    athena.discover_views = MagicMock()
    athena.execute_query = MagicMock()
    athena.get_view_diff = MagicMock(return_value=None)
    with IsolatedParameters(), patch('cid.helpers.athena.isatty', return_value=terminal), \
            patch('cid.helpers.athena.logger.trace', create=True):
        set_parameters({'on-drift': on_drift})
        athena.create_or_update_view('my_view', 'CREATE OR REPLACE VIEW my_view AS SELECT 1')
    return athena


def test_override_in_terminal_replaces_view():
    athena = _run('override', terminal=True)
    athena.get_view_diff.assert_not_called()
    athena.execute_query.assert_called_once()


def test_no_terminal_keeps_view():
    for on_drift in ('show', 'override'):
        athena = _run(on_drift, terminal=False)
        athena.execute_query.assert_not_called()
