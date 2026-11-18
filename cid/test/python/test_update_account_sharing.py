""" Unit tests for Cid.update_account_sharing (cid/common.py)

On update, sharing with everyone in the account must change only when
share-with-account is explicitly provided: 'yes' grants, 'no' revokes the
namespace grant (regular and link permissions) if present.
"""
from unittest.mock import MagicMock

import pytest

from cid.common import Cid
from cid.exceptions import CidCritical
from cid.utils import IsolatedParameters, set_parameters

ACCOUNT_ID = '123456789012'
NAMESPACE = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:namespace/default'
USER = f'arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/admin'
ACTIONS = ['quicksight:DescribeDashboard', 'quicksight:ListDashboardVersions', 'quicksight:QueryDashboard']


def _cid(permissions=None, link_permissions=None):
    """ Cid with a mocked QuickSight helper """
    cid = Cid.__new__(Cid)
    cid.share = MagicMock()
    qs = MagicMock()
    qs.account_id = ACCOUNT_ID
    qs.client.exceptions.ClientError = Exception
    qs.get_dashboard_permissions.return_value = permissions or []
    qs.get_dashboard_link_permissions.return_value = link_permissions or []
    cid.__dict__['qs'] = qs # bypass cached_property
    return cid


def _run(cid, value=None):
    with IsolatedParameters():
        if value is not None:
            set_parameters({'share-with-account': value})
        cid.update_account_sharing('dash')


def test_not_provided_does_nothing():
    cid = _cid(permissions=[{'Principal': NAMESPACE, 'Actions': ACTIONS}])
    _run(cid)
    cid.share.assert_not_called()
    cid.qs.update_dashboard_permissions.assert_not_called()


def test_yes_grants():
    for value in ('yes', True):
        cid = _cid()
        _run(cid, value)
        cid.share.assert_called_once_with('dash')


def test_no_revokes_namespace_only():
    user_perm = {'Principal': USER, 'Actions': ACTIONS + ['quicksight:UpdateDashboardPermissions']}
    namespace_perm = {'Principal': NAMESPACE, 'Actions': ACTIONS}
    cid = _cid(permissions=[user_perm, namespace_perm], link_permissions=[namespace_perm])
    _run(cid, 'no')
    cid.share.assert_not_called()
    cid.qs.update_dashboard_permissions.assert_called_once_with(
        DashboardId='dash',
        RevokePermissions=[namespace_perm],
        RevokeLinkPermissions=[namespace_perm],
    )


def test_no_without_grant_does_nothing():
    cid = _cid(permissions=[{'Principal': USER, 'Actions': ACTIONS}])
    _run(cid, 'no')
    cid.qs.update_dashboard_permissions.assert_not_called()


def test_no_fails_loudly_if_permissions_cannot_be_read():
    cid = _cid()
    cid.qs.get_dashboard_permissions.side_effect = Exception('AccessDeniedException')
    with pytest.raises(CidCritical):
        _run(cid, 'no')


def test_no_fails_loudly_if_revoke_fails():
    cid = _cid(permissions=[{'Principal': NAMESPACE, 'Actions': ACTIONS}])
    cid.qs.update_dashboard_permissions.side_effect = Exception('AccessDeniedException')
    with pytest.raises(CidCritical):
        _run(cid, 'no')
