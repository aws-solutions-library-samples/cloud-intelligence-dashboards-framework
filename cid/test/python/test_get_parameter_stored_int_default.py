""" Unit tests for text parameters whose stored default is a number (cid/utils.py)

Parameters saved by a previous deploy are read back from Athena with json.loads
(cid/helpers/parameter_store.py), so a text value such as '7' comes back as int 7.
InquirerPy text prompts only accept a str default, so get_parameter must hand
over a str. Regression test for `cid-cmd update --force --recursive` failing at
the summary_view retention_months parameter.
"""
from unittest.mock import patch

import pytest
from InquirerPy.prompts.input import InputPrompt

import cid.utils
from cid.helpers.parameter_store import ParametersController
from cid.utils import IsolatedParameters, get_parameter, unset_parameter

PARAM = 'view-summary-view-retention-months'


def _stored_default(value):
    """ default as loaded from the cid_parameters view for a value saved as text """
    controller = ParametersController.__new__(ParametersController)  # no Athena needed
    rows = [{'parameter': PARAM, 'value': value, 'context': 'None', 'date': '2026-10-10 00:00:00'}]
    with patch.object(ParametersController, 'load', return_value=rows):
        return controller.load_parameters(context=None)[PARAM]


def _get(default, all_yes=False, interactive=True):
    with IsolatedParameters(), patch.dict(cid.utils.defaults, {PARAM: default}), patch.object(cid.utils, '_all_yes', all_yes):
        unset_parameter(PARAM)
        # real InquirerPy argument validation; only the terminal interaction is replaced by "user presses Enter"
        with patch('cid.utils.isatty', return_value=interactive), \
                patch.object(InputPrompt, 'execute', lambda self: self._default):
            return get_parameter(PARAM, message='months', default='7')


def test_stored_text_number_is_loaded_as_int():
    assert _stored_default('7') == 7


def test_interactive_prompt_accepts_stored_int_default():
    assert _get(_stored_default('7')) == '7'


def test_interactive_prompt_str_default_unchanged():
    assert _get('12') == '12'


def test_unattended_takes_stored_int_default_as_text():
    assert _get(_stored_default('7'), all_yes=True, interactive=False) == '7'


def test_unattended_without_yes_still_fails():
    with pytest.raises(Exception, match='Please set parameter'):
        _get(_stored_default('7'), all_yes=False, interactive=False)
