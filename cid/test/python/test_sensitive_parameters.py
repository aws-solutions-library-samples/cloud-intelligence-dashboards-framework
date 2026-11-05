""" Credentials must never be echoed in the 'Next time you can use following command' line or logs """
from cid.cli import command_line
from cid.utils import IsolatedParameters, get_loggable_parameters, is_sensitive_parameter, set_parameters

SECRETS = {
    'aws_access_key_id': 'ASIAEXAMPLEKEYID0000',  # nosec B105 - fake test value
    'aws_secret_access_key': 'exampleSecretKey/0000+0000',  # nosec B105 - fake test value
    'aws_session_token': 'IQoJexampleSessionToken0000',  # nosec B105 - fake test value
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


def test_next_time_command_line_has_no_credentials():
    """ credentials passed on the command line or injected by a plugin (e.g. cid-internal) are left out """
    cli_params = {'dashboard_id': 'cudos-v5', 'aws_session_token': SECRETS['aws_session_token'], 'recursive': True}
    params = {k.replace('_', '-'): v for k, v in SECRETS.items()}
    params['timezone'] = 'Europe/London'
    line = command_line('deploy', cli_params, params)
    assert line.startswith('cid-cmd deploy')
    assert "--dashboard-id 'cudos-v5'" in line and '--recursive' in line and "--timezone 'Europe/London'" in line
    for value in SECRETS.values():
        assert value not in line
    assert 'aws-session-token' not in line and 'aws-secret-access-key' not in line
