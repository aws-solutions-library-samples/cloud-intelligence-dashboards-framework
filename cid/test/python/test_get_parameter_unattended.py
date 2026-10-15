""" Unit tests for get_parameter text entry without a terminal (cid/utils.py)

In unattended mode (-y / Lambda) a missing text parameter takes its default.
Without a default, or without -y, it still fails.
"""
from unittest.mock import patch

import pytest

import cid.utils
from cid.utils import IsolatedParameters, get_parameter, unset_parameter


def _get(default, all_yes, template_variables={}):
    with IsolatedParameters(), patch.object(cid.utils, '_all_yes', all_yes):
        unset_parameter('test-text')
        with patch('cid.utils.isatty', return_value=False):
            return get_parameter('test-text', message='enter', default=default, template_variables=template_variables)


def test_unattended_uses_default():
    assert _get('s3://bucket/path', all_yes=True) == 's3://bucket/path'


def test_unattended_formats_default():
    assert _get('s3://cid-data-{account_id}/x', all_yes=True, template_variables={'account_id': '123'}) == 's3://cid-data-123/x'


def test_unattended_without_default_fails():
    with pytest.raises(Exception, match='Please set parameter test-text'):
        _get(None, all_yes=True)


def test_without_yes_fails_even_with_default():
    with pytest.raises(Exception, match='Please set parameter test-text'):
        _get('s3://bucket/path', all_yes=False)
