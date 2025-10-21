import os
import sys
import csv
import copy
import math
import inspect
import logging
import platform
import datetime
from typing import Any, Dict
from functools import lru_cache as cache
from collections.abc import Iterable

import yaml
import requests
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator
from InquirerPy.prompts import InputPrompt
from prompt_toolkit.validation import ValidationError, Validator
from boto3.session import Session
from botocore.exceptions import NoCredentialsError, CredentialRetrievalError, NoRegionError, ProfileNotFound

from cid.exceptions import CidCritical

logger = logging.getLogger(__name__)

defaults = {} # params from parameter storage
params = {} # parameters from command line
_all_yes = False # parameters from command line

PYPI_URL = "https://pypi.org/pypi/cid-cmd/json"

def get_latest_tool_version():
    ''' call PyPI url to get the latest version of the package
    '''
    res_json = {}
    try:
        r = requests.get(PYPI_URL,timeout=3)
        r.raise_for_status()
        res_json = r.json()
    except requests.exceptions as exec:
        logger.debug(exec, exc_info=True)
    finally:
        return res_json.get("info", {}).get("version", "UNDEFINED")

@cache(maxsize=None)
def all_yes():
    return _all_yes


@cache(maxsize=None)
def isatty():
    """ return True if executed in a Terminal that allows user input """
    if exec_env()['terminal'] == 'gitbash': # We cannot trust isatty on Git Bash on Windows
        return True
    return sys.__stdin__.isatty()

@cache(maxsize=None)
def exec_env():
    """ return os, shell and terminal
    supported environments: lambda, CloudShell, macos terminals, windows/cmd, windows/powershell, windows/gitbash
    """
    terminal = 'unknown'
    shell = 'unknown'
    os_ = platform.system()
    if os.environ.get('AWS_EXECUTION_ENV', '').startswith('AWS_Lambda'):
        terminal = 'lambda'
        shell = 'lambda'
    elif os.environ.get('AWS_EXECUTION_ENV', '') == 'CloudShell':
        terminal = 'cloudshell'
        shell = 'cloudshell'
    elif os.environ.get('SHELL', '').endswith('bash.exe'): # gitbash
        terminal = 'gitbash'
        shell = 'bash'
    elif os.environ.get('TERM_PROGRAM', ''):  # macos
        terminal = os.environ.get('TERM_PROGRAM', '')
        shell = os.environ.get('SHELL', '')
    elif os.environ.get('COMSPEC', '').endswith('cmd.exe'): # cmd
        terminal = 'cmd'
        shell = 'cmd'
    elif os.environ.get('PSMODULEPATH', ''): # powershell
        terminal = 'powershell'
        shell = 'powershell'
    return {'os': os_, 'shell': shell, 'terminal': terminal}

def split_respecting_quotes(s):
    """ split respecting quotes
    """
    return next(csv.reader([s]))

def intersection(a: Iterable, b: Iterable) -> Iterable:
    """ intersection of 2 arrays
    """
    return sorted(set(a).intersection(b))

def difference(a: Iterable, b: Iterable) -> Iterable:
    """ difference of 2 arrays
    """
    return sorted(list(set(a).difference(b)))

def get_aws_region() -> str:
    """ get aws region
    """
    return get_boto_session().region_name

def get_boto_session(**kwargs) -> Session:
    """
    Initiates boto's session object
    :param region: region name
    :param args: arguments
    :return: Boto3 Client
    """
    try:
        session = Session(**kwargs)
        logger.info('Boto3 session created')
        logger.debug(session)
        if not session.region_name:
            raise NoRegionError
        return session
    except (NoCredentialsError, CredentialRetrievalError):
        print('Error: unable to initialize session, please check your AWS credentials, exiting')
        exit(1)
    except NoRegionError:
        logger.info('No AWS region set, defaulting to us-east-1')
        kwargs.update({'region_name': 'us-east-1'})
        return get_boto_session(**kwargs)
    except ProfileNotFound as e:
        logger.critical(e)
        exit(1)
    except Exception as e:
        logger.debug(e, exc_info=True)
        raise

def get_boto_client(service_name, **kwargs):
    """
    Initiates boto's client object
    :param service_name: service name
    :param region: region name
    :param args: arguments
    :return: Boto3 Client
    """
    try:
        session = get_boto_session(**kwargs)
        return session.client(service_name)
    except (NoCredentialsError, CredentialRetrievalError):
        raise CidCritical('Error: unable to initialize boto client, please check your AWS credentials, exiting')
    except Exception as e:
        logger.debug(e, exc_info=True)
        raise


def cid_print(value, **kwargs) -> None:
    ''' Print AND log
    ex:
        violets, roses = 'violets', 'roses'
        cid_print(f'{roses} are <BOLD><RED>red<END>, {violets} are <BLUE><UNDERLINE>blue<END>')

    '''
    colors = {
        'PURPLE': '\033[95m',
        'CYAN': '\033[96m',
        'GREY': '\033[90m',
        'DARKCYAN': '\033[36m',
        'BLUE': '\033[94m',
        'GREEN': '\033[92m',
        'YELLOW': '\033[93m',
        'RED': '\033[91m',
        'BOLD': '\033[1m',
        'UNDERLINE': '\033[4m',
        'END': '\033[0m',
    }

    msg = str(value)
    log_msg = str(msg)
    for col, val in colors.items():
        msg = msg.replace(f'<{col}>', val)
        log_msg = log_msg.replace(f'<{col}>', '')
    try:
        mod = inspect.getmodule(inspect.stack()[1][0])
        name = mod.__name__ if mod else __name__
        logging.getLogger(name).debug(log_msg)
    except Exception as exc:
        logger.debug('cid_print: {exc}')
    print(msg, **kwargs)

def set_defaults(data: dict) -> None:
    global defaults
    logger.debug(f'setting defaults to: {data}')
    if data:
        defaults.update(data)

def get_defaults() -> dict:
    global defaults
    return dict(defaults)

def set_parameters(parameters: dict, all_yes: bool=None) -> None:
    for k, v in parameters.items():
        params[k.replace('_', '-')] = v

    if all_yes != None:
        global _all_yes
        _all_yes = all_yes
        logger.debug(f'all_yes={all_yes}')

def get_parameters():
    global params
    return dict(params)

def get_yesno_parameter(param_name: str, message: str, default: str=None, break_on_ctrl_c=True):
    logger.debug(f'getting param {param_name}')
    param_name = param_name.replace('_', '-')
    mapping = {True: True, False: False, 'yes': True, 'no': False}
    if param_name in params:
        param_value = params.get(param_name)
        if param_value is not None:
            return mapping[param_value]
        else:
            unset_parameter(param_name)
    if default is not None:
        default = default.lower()
        default = 'yes' if mapping.get(default) else 'no'

    if _all_yes:
        params[param_name] = True
    else:
        res = get_parameter(param_name, message=message, choices=['yes', 'no'], default=default, break_on_ctrl_c=break_on_ctrl_c, fuzzy=False)
        params[param_name] = (res == 'yes')
    return params[param_name]


def get_parameter(param_name, message, choices=None, default=None, none_as_disabled=False, template_variables={}, break_on_ctrl_c=True, fuzzy=True, multi=False, order=False, yes_choice='yes'):
    """
    Check if parameters are provided in the command line and if not, ask user

    :param message: text message for user
    :param choices: a list or dict for choice. None for text entry. Keys and Values must be strings.
    :param default: a default text template
    :param none_as_disabled: if True and choices is a dict, all choices with None as a value will be disabled
    :param template_variables: a dict with variables for template
    :param break_on_ctrl_c: if True, exit() if user pressed CTRL+C
    :param fuzzy: if we need to use fuzzy input
    :param multi: if we need multiple items as output
    :param order: if we need to make order

    :returns: a value from user or provided in command line
    """
    global defaults, params
    logger.debug(f'getting param {param_name}')
    param_name = param_name.replace('_', '-')

    # override defaults from code with outside defaults
    if param_name in get_defaults():
        default = defaults.get(param_name)
        if multi and isinstance(default, str):
            default = split_respecting_quotes(default)
        logger.debug(f'using default {param_name} = {default}')

    if params.get(param_name):
        value = params[param_name]
        logger.debug(f'Using {param_name}={value}, from parameters')
        if isinstance(value, str) and template_variables:
            try:
                value = value.format(**template_variables)
            except KeyError:
                pass
        if multi and isinstance(value, str):
            value = split_respecting_quotes(value)
        return value

    if choices is not None:
        if _all_yes and (yes_choice in choices):
            logger.debug(f'Using {yes_choice}, as -y flag is active')
            return yes_choice
        print()
        if multi:
            default = default or []
            default = default if isinstance(default, list) else [default]
            default = [c for c in default if c in choices]
            if not isatty():
                result = default
            else:
                result = select_and_order(message, choices, default)
        else:
            if not isatty():
                raise Exception(f'Please set parameter {param_name}. Unable to request user in environment={exec_env()}')
            if isinstance(choices, dict):
                choices = [Choice(name=key, value=value, enabled=not (none_as_disabled and value is None)) for key, value in choices.items()]
            elif isinstance(choices, list):
                choices = [Choice(name=key, value=key, enabled=True) for key in choices]

            if fuzzy:
                result = inquirer.fuzzy(
                    message=f'[{param_name}] {message}:',
                    choices=sorted(choices, key=lambda x: (x.value != default)),  # Make default as the first one
                    long_instruction='Type to search or use arrows ↑↓ to navigate',
                    match_exact=True,
                    exact_symbol='',
                ).execute()
            else:
                result = inquirer.select(
                    message=f'[{param_name}] {message}:',
                    choices=choices,
                    long_instruction='Use arrows ↑↓ to navigate',
                    default=default,
                ).execute()
    else: # it is a text entry
        if isinstance(default, str) and template_variables:
            logger.debug(f'template_variables = {template_variables}')
            default=default.format(**template_variables)
        print()
        if not isatty():
            raise Exception(f'Please set parameter {param_name}. Unable to request user in environment={exec_env()}')
        result = inquirer.text(
            message=f'[{param_name}] {message}:' ,
            default=default or '',
        ).execute()
        if isinstance(result, str) and template_variables:
            result = result.format(**template_variables)
    if (break_on_ctrl_c and result is None):
        exit(1)
    logger.info(f"(Use \033[1m --{param_name} '{result}'\033[0m next time you run this)")
    params[param_name] = result
    return result

def unset_parameter(param_name):
    param_name = param_name.replace('_', '-')
    if param_name in params:
        value = params[param_name]
        del params[param_name]
        logger.info(f'Cleared {param_name}={value}, from parameters')


def ago(time):
    """ Calculate a '3 hours ago' type string from a python datetime.
    credits: https://gist.github.com/tonyblundell/2652369
    """
    units = {
        'days': lambda diff: diff.days,
        'hours': lambda diff: diff.seconds / 3600,
        'minutes': lambda diff: diff.seconds % 3600 / 60,
    }
    diff = datetime.datetime.now().replace(tzinfo=time.tzinfo) - time
    for unit in units:
        dur = math.floor(units[unit](diff)) # Run the lambda function to get a duration
        if dur > 0:
            unit = unit[:-dur] if dur == 1 else unit # De-pluralize if duration is 1 ('1 day' vs '2 days')
            return '%s %s ago' % (dur, unit)
    return 'just now'


class IsolatedParameters:
    """A context manager to run something in isolated set of parameters"""
    def __enter__(self):
        self.backup = copy.deepcopy(params)

    def __exit__(self, exc_type, exc_value, traceback):
        global params
        params = self.backup

def merge_objects(obj1, obj2, depth=2):
    """ merging objects with a depth

    unit tests: cid/test/python/test_merge.py
    """
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        result = obj1.copy()
        for key, value in obj2.items():
            if depth > 0 and key in result and isinstance(result[key], (dict, list)) and isinstance(value, (dict, list)):
                result[key] = merge_objects(result[key], value, depth - 1)
            else:
                result[key] = value
        return result
    elif isinstance(obj1, list) and isinstance(obj2, list):
        return obj1 + obj2
    else:
        return obj2  # If types don't match or if one of them is not a dict or list, prefer the second object.


def select_items(message, all_items, selected_items=[]):
    """Let user select which items they want from all available items"""
    # preserve the order:
    valid_selected = [item for item in selected_items if item in all_items]
    remaining = [item for item in all_items if item not in valid_selected]
    all_items = valid_selected + remaining

    return inquirer.checkbox(
        message=f"{message}:",
        long_instruction='(use SPACE to select, ENTER to continue, arrows ↑↓ to navigate)',
        choices=[Choice(item, name=item, enabled=item in selected_items) for item in all_items],
        transformer=lambda result: f"{len(result)} items selected"
    ).execute()


def select_and_order(message, all_items, selected_items=None):
    """Let user select and arrange items from a list"""
    selected_items = (selected_items or []).copy()
    unselected_items = [item for item in all_items if item not in selected_items]

    while True:
        choices = []
        #choices.append(Choice(value={"action": "back"}, name="⬅ Go back"))
        choices.append(Choice(value={"action": "finish"}, name="✔ Looks good"))
        choices.append(Separator("--- Selected Items ---"))

        if selected_items:
            for i, item in enumerate(selected_items):
                choices.append(Choice(
                    value={"action": "manage_selected", "index": i, "item": item},
                    name=f"✓ [{i+1}] {item}"
                ))
        else:
            choices.append(Separator("--- Selected Items (empty) ---"))
        if unselected_items:
            choices.append(Separator("--- Items Available To Add ---"))
        for item in unselected_items:
            choices.append(Choice(
                value={"action": "add_item", "item": item},
                name=f"  {item}"
            ))
        selection = inquirer.select(
            message=message,
            choices=choices,
        ).execute()

        if selection["action"] == 'back':
            raise KeyboardInterrupt('getting back')
        if selection["action"] == 'finish':
            break
        print('\033[K\033[F', end='')  # Clear line
        if selection["action"] == 'add_item':
            # Add unselected item to selected list
            item_to_add = selection["item"]
            selected_items.append(item_to_add)
            unselected_items.remove(item_to_add)
        elif selection["action"] == 'manage_selected':
            # Manage selected item (move or delete)
            item_index = selection["index"]
            selected_item = selection["item"]
            # Ask what action the user wants to take with this item
            actions = []
            if item_index > 0:
                actions.append(Choice(value="top", name=f"⬆⬆ Move {selected_item} to top"))
                actions.append(Choice(value="up", name=f"⬆  Move {selected_item} up one position"))
            if item_index < len(selected_items) - 1:
                actions.append(Choice(value="down", name=f"⬇  Move {selected_item} down one position"))
                actions.append(Choice(value="bottom", name=f"⬇⬇ Move {selected_item} to bottom"))
            actions.append(Choice(value="delete", name=f"❌  Remove {selected_item} from selection"))
            actions.append(Choice(value="cancel", name="Cancel (no change)"))
            action = inquirer.select(
                message=f"What would you like to do with '{selected_item}'?",
                choices=actions,
            ).execute()

            if action == "top" and item_index > 0:
                selected_items.insert(0, selected_items.pop(item_index))
            elif action == "up" and item_index > 0:
                selected_items[item_index], selected_items[item_index-1] = selected_items[item_index-1], selected_items[item_index]
            elif action == "down" and item_index < len(selected_items) - 1:
                selected_items[item_index], selected_items[item_index+1] = selected_items[item_index+1], selected_items[item_index]
            elif action == "bottom" and item_index < len(selected_items) - 1:
                selected_items.append(selected_items.pop(item_index))
            elif action == "delete":
                removed_item = selected_items.pop(item_index)
                unselected_items.append(removed_item)
            elif action == "cancel":
                pass
            print('\033[K\033[F', end='')  # Clear line
    return selected_items
