""" Credentials must never be echoed in the 'Next time you can use following command' line or logs """
import logging

from click.testing import CliRunner

import cid.cli
from cid.utils import IsolatedParameters, get_loggable_parameters, is_sensitive_parameter, set_parameters

SECRETS = {
    'aws_access_key_id': 'ASIAEXAMPLEKEYID0000',
    'aws_secret_access_key': 'exampleSecretKey/0000+0000',
    'aws_session_token': 'IQoJexampleSessionToken0000',
}


def test_is_sensitive_parameter():
    for name in ('aws_access_key_id', 'aws-secret-access-key', 'AWS_SESSION_TOKEN'):
        assert is_sensitive_parameter(name)
    assert not is_sensitive_parameter('dashboard-id')


def test_loggable_parameters_mask_credentials():
    with IsolatedParameters():
        set_parameters({**SECRETS, 'dashboard_id': 'cudos-v5'})
        safe = get_loggable_parameters()
    assert safe['dashboard-id'] == 'cudos-v5'
    assert all(safe[k.replace('_', '-')] == '***' for k in SECRETS)


def test_next_time_command_line_has_no_credentials(caplog, monkeypatch):
    """ a plugin (e.g. cid-internal) injects federated credentials as parameters during the command """
    @cid.cli.cid_command
    def fake_cmd(ctx, **kwargs):
        set_parameters(SECRETS)

    runner = CliRunner()
    with IsolatedParameters(), caplog.at_level(logging.INFO, logger='cid'):
        runner.invoke(cid.cli.main, ['fake-cmd', '--dashboard-id', 'cudos-v5'], obj=None, catch_exceptions=True)
    text = caplog.text
    assert 'cid-cmd fake-cmd' in text, f'command line was not logged: {text!r}'
    assert '--dashboard-id' in text
    for value in SECRETS.values():
        assert value not in text
