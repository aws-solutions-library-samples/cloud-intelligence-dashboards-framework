""" Unit tests for tags_json view parameters (cid/common.py)

A tags_json parameter (ex: FOCUS resource_tags) must render SQL from tag names
whether they are passed by the user or selected interactively. Without a
terminal and without tags it renders an empty json.
"""
from unittest.mock import MagicMock, patch

from cid.common import Cid
from cid.utils import IsolatedParameters, set_parameters

PARAMETERS = {'resource_tags': {'type': 'tags_json', 'query': 'SELECT tags', 'global': True}}
TAGS = [["Tags['team']"], ["Tags['cost:center']"]]


def _render(resource_tags=None, terminal=False):
    cid = Cid.__new__(Cid)
    athena = MagicMock()
    athena.query.return_value = TAGS
    athena.client.exceptions.ClientError = Exception
    cid.__dict__['athena'] = athena # bypass cached_property
    with IsolatedParameters(), patch('cid.utils.isatty', return_value=terminal), \
            patch('cid.common.logger.trace', create=True):
        if resource_tags is not None:
            set_parameters({'resource-tags': resource_tags})
        return cid.get_template_parameters(PARAMETERS)['resource_tags'], athena


def test_passed_tags_render_sql():
    sql, athena = _render('tag_team,tag_cost_center')
    athena.query.assert_called_once()
    assert "('tag_team', Tags['team'])" in sql
    assert "('tag_cost_center', Tags['cost:center'])" in sql
    assert '[' not in sql.split('ARRAY')[0], 'raw python list must not be rendered'


def test_unknown_tags_are_skipped():
    sql, _ = _render('tag_team,tag_missing')
    assert "('tag_team', Tags['team'])" in sql
    assert 'tag_missing' not in sql


def test_no_terminal_no_tags_renders_empty_json():
    sql, athena = _render()
    assert sql == "'{}'"
    athena.query.assert_not_called()
