""" Unit tests for get_parameter multi-select choice shapes (cid/utils.py)

get_parameter(multi=True) accepts several shapes of `choices`:
 1. flat list:           ['a', 'b']
 2. dict display->value: {'a [resource tag]': 'a'}
 3. grouped lists:       {'group': ['a', 'b']}
 4. grouped dicts:       {'group': {'a [tag_x + tag_y]': 'a'}}  (per-item display labels)
Mixed 3+4 is allowed: {'group1': {'display': 'a'}, 'group2': ['b']}

All shapes must return VALUES (not display labels) and resolve defaults given
as values. Interactive tests mock select_and_order to verify what is displayed;
non-interactive tests verify the default resolution path.
"""
from unittest.mock import patch

from cid.utils import IsolatedParameters, get_parameter, unset_parameter


def _get(choices, default, select_and_order_mock=None, interactive=False):
    """ run get_parameter(multi=True) in an isolated parameter context """
    with IsolatedParameters():
        unset_parameter('test-choices')
        with patch('cid.utils.isatty', return_value=interactive):
            return get_parameter('test-choices', message='pick', choices=choices, default=default, multi=True)


# ---------------------------------------------------------------------------
# non-interactive: defaults resolve to values for every shape
# ---------------------------------------------------------------------------

def test_flat_list():
    assert _get(['a', 'b', 'c'], default=['a', 'c']) == ['a', 'c']


def test_flat_list_unknown_defaults_dropped():
    assert _get(['a', 'b'], default=['a', 'nonexistent']) == ['a']


def test_dict_display_to_value():
    choices = {'a [resource tag]': 'a', 'b [account tag]': 'b'}
    assert _get(choices, default=['b']) == ['b']


def test_grouped_lists():
    choices = {'resource tags': ['tag_a', 'tag_b'], 'account mapping': ['application']}
    assert _get(choices, default=['tag_a', 'application']) == ['tag_a', 'application']


def test_grouped_dicts_with_display_labels():
    """ the shape used by the taxonomy prompt for merged columns:
    the display label carries the composition, the value is the column name """
    choices = {'merged columns': {'app_merged [tag_a + application]': 'app_merged'}}
    assert _get(choices, default=['app_merged']) == ['app_merged']


def test_grouped_mixed_dict_and_list():
    choices = {
        'merged columns': {'app_merged [tag_a + application]': 'app_merged'},
        'account mapping': ['application', 'environment'],
    }
    assert _get(choices, default=['app_merged', 'environment']) == ['app_merged', 'environment']


def test_grouped_unknown_defaults_dropped():
    choices = {'group': ['a'], 'group2': {'b [x]': 'b'}}
    assert _get(choices, default=['a', 'b', 'gone']) == ['a', 'b']


# ---------------------------------------------------------------------------
# interactive: what is displayed vs what is returned
# ---------------------------------------------------------------------------

@patch('cid.utils.select_and_order')
def test_interactive_grouped_dicts_display_labels_shown_values_returned(select_mock):
    """ REGRESSION: merged columns must be displayed with their [composition]
    and the returned result must be the bare value """
    choices = {
        'merged columns': {'app_merged [tag_a + application]': 'app_merged'},
        'account mapping': ['application'],
    }
    # user selects both items (select_and_order returns display labels)
    select_mock.return_value = ['app_merged [tag_a + application]', 'application']
    result = _get(choices, default=[], interactive=True)
    assert result == ['app_merged', 'application']
    # display labels with composition were offered to the user
    groups_shown = select_mock.call_args.kwargs.get('groups') or select_mock.call_args.args[3]
    assert groups_shown == {
        'merged columns': ['app_merged [tag_a + application]'],
        'account mapping': ['application'],
    }


@patch('cid.utils.select_and_order')
def test_interactive_grouped_defaults_translated_to_labels(select_mock):
    """ defaults arrive as values and must be pre-selected via their display labels """
    choices = {'merged columns': {'app_merged [tag_a]': 'app_merged'}}
    select_mock.return_value = []
    _get(choices, default=['app_merged'], interactive=True)
    preselected = select_mock.call_args.args[2]
    assert preselected == ['app_merged [tag_a]']


@patch('cid.utils.select_and_order')
def test_interactive_dict_display_to_value(select_mock):
    choices = {'a [resource tag]': 'a', 'b [account tag]': 'b'}
    select_mock.return_value = ['b [account tag]']
    assert _get(choices, default=[], interactive=True) == ['b']
    displayed = select_mock.call_args.args[1]
    assert displayed == ['a [resource tag]', 'b [account tag]']


@patch('cid.utils.select_and_order')
def test_interactive_flat_list_passthrough(select_mock):
    select_mock.return_value = ['b']
    assert _get(['a', 'b'], default=[], interactive=True) == ['b']
    assert select_mock.call_args.kwargs.get('groups') is None
