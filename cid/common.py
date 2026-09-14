import os
import re
import json
import shutil
import time
import urllib
import logging
import functools
import webbrowser
from string import Template
from typing import Dict
from importlib import resources
from importlib.metadata import entry_points
from functools import cached_property

import yaml
import requests
from botocore.exceptions import ClientError, NoCredentialsError, CredentialRetrievalError
from InquirerPy.separator import Separator


from cid import utils
from cid.base import CidBase
from cid.plugin import Plugin
from cid.utils import get_parameter, get_parameters, set_parameters, unset_parameter, get_yesno_parameter, cid_print, isatty, merge_objects, IsolatedParameters, set_defaults
from cid.helpers.account_map import AccountMap
from cid.helpers.account_mapper import AccountMapper
from cid.helpers.parameter_store import ParametersController
from cid.helpers.taxonomy_merger import TaxonomyMerger
from cid.helpers import Athena, S3, IAM, CUR, ProxyCUR, Glue, QuickSight, Dashboard, Dataset, Datasource, csv2view, Organizations, CFN
from cid.helpers.quicksight.template import Template as CidQsTemplate
from cid.helpers.quicksight.space import Space
from cid.helpers.quicksight.agent import Agent
from cid.helpers.quicksight.agent_logic import (
    build_agent_listing,
    build_console_url,
    classify_dependencies,
    derive_space_id,
    describe_drift,
    is_cid_managed,
    select_database_from_candidates,
    validate_caps,
)
from cid._version import __version__
from cid.export import export_analysis
from cid.logger import set_cid_logger
from cid.exceptions import CidError, CidCritical
from cid.commands import InitQsCommand

logger = logging.getLogger(__name__)

class Cid():

    def __init__(self, **kwargs) -> None:
        self.base: CidBase = None
        # Defined resources
        self.resources = dict()
        self.dashboards = dict()
        self.plugins = self.__loadPlugins()
        self._clients = dict()
        self._visited_views = [] # Views updated in the current session
        self.qs_url = 'https://{region}.quicksight.{domain}/sn/dashboards/{dashboard_id}'
        self.all_yes = kwargs.get('yes')
        self.verbose = kwargs.get('verbose')
        # save main parameters to global parameters but do not override parameters that were set from outside
        set_parameters({key: val for key, val in kwargs.items() if key.replace('_', '-') not in get_parameters()}, self.all_yes)
        self._logger = None
        self.catalog_urls = [
            'https://raw.githubusercontent.com/aws-samples/aws-cudos-framework-deployment/main/dashboards/catalog.yaml',
            'https://raw.githubusercontent.com/aws-samples/aws-cudos-framework-deployment/main/agents/catalog.yaml',
        ]

    def aws_login(self):
        params = {
            'profile_name': None,
            'region_name': None,
            'aws_access_key_id': None,
            'aws_secret_access_key': None,  #nosec B105
            'aws_session_token': None  #nosec B105
        }
        for key in params.keys():
            value = get_parameters().get(key.replace('_', '-'), '<NO VALUE>')
            if  value != '<NO VALUE>':
                params[key] = value
        if get_parameters().get('region'):
            params['region_name'] = get_parameters().get('region') # use region as a synonym of region_name

        print('Checking AWS environment...')
        try:
            self.base = CidBase(session=utils.get_boto_session(**params))
            if self.base.session.profile_name:
                print(f'\tprofile name: {self.base.session.profile_name}')
                logger.info(f'AWS profile name: {self.base.session.profile_name}')
            self.qs_url_params = {
                'account_id': self.base.account_id,
                'region': self.base.session.region_name,
                'domain': self.base.domain,
            }
        except (NoCredentialsError, CredentialRetrievalError):
            raise CidCritical('Error: Not authenticated, please check AWS credentials')
        except ClientError as e:
            raise CidCritical(f'ClientError: {e}')
        print(f'\taccountId: {self.base.account_id}\n\tAWS userId: {self.base.username}')
        logger.info(f'AWS accountId: {self.base.account_id}')
        logger.info(f'AWS userId: {self.base.username}')
        print('\tRegion: {}'.format(self.base.session.region_name))
        logger.info(f'AWS region: {self.base.session.region_name}')
        print('\n')

    @cached_property
    def qs(self) -> QuickSight:
        return QuickSight(self.base.session, resources=self.resources)

    @cached_property
    def space(self) -> Space:
        return Space(self.base.session, resources=self.resources)

    @cached_property
    def agent(self) -> Agent:
        return Agent(self.base.session, resources=self.resources)

    @cached_property
    def athena(self) -> Athena:
        return Athena(self.base.session, resources=self.resources)

    @cached_property
    def glue(self) -> Glue:
        return Glue(self.base.session)

    @cached_property
    def iam(self) -> IAM:
        return IAM(self.base.session)

    @cached_property
    def cfn(self) -> CFN:
        return CFN(self.base.session)

    @cached_property
    def organizations(self) -> Organizations:
        return Organizations(self.base.session)

    @cached_property
    def s3(self) -> S3:
        return S3(self.base.session)

    @cached_property
    def cur1(self):
        """ get/create a cur1 """
        return self.get_cur('1')

    @cached_property
    def cur2(self):
        """ get/create a cur2 """
        return self.get_cur('2')

    @cached_property
    def parameters_controller(self):
        """ get/create parameters_controller """
        return ParametersController(self.athena)

    def get_cur(self, target_cur_version=None):
        """ get a cur """
        cur_version = self.cur.version
        if (target_cur_version and cur_version != target_cur_version) or get_parameters().get('use-cur-proxy'):
            return ProxyCUR(self.cur, target_cur_version=target_cur_version)
        return self.cur

    @property
    def cur(self) -> CUR:
        '''can return any CUR (1 or 2) that customer provides'''
        if not self._clients.get('cur'):
            while True:
                try:
                    _cur = CUR(self.athena, self.glue)
                    print('Checking if CUR is enabled and available...')

                    if not _cur.metadata:
                        raise CidCritical("Error: please ensure CUR is enabled, if yes allow it some time to propagate")

                    cid_print(f'\tAthena table: {_cur.table_name}')
                    cid_print(f"\tResource IDs: {'yes' if _cur.has_resource_ids else 'no'}")
                    if not _cur.has_resource_ids:
                        raise CidCritical("Error: CUR has to be created with Resource IDs")
                    cid_print(f"\tSavingsPlans: {'yes' if _cur.has_savings_plans else 'no'}")
                    cid_print(f"\tReserved Instances: {'yes' if _cur.has_reservations else 'no'}")
                    cid_print('\n')
                    self._clients['cur'] = _cur
                    break
                except CidCritical:
                    if not utils.isatty():
                        raise # do not allow CUR creation in lambda
                    cid_print(f'CUR not found in {self.athena.DatabaseName}. If you have S3 bucket with CUR in this account you can create a CUR table with Crawler.')
                    self.create_cur_table()
        return self._clients['cur']

    def create_or_update_account_map(self, name):
        account_map = AccountMap(
            self.base.session,
            self.athena,
            self.get_cur, # can be any CUR. But it is only needed for trends and dummy
        )
        return account_map.create_or_update(name)

    def command(func):
        ''' a decorator that ensure that we logged in to AWS acc, and loaded additional resource files
        '''
        @functools.wraps(func)
        def wrap(self, *args, **kwargs):
            self.all_yes = self.all_yes or kwargs.get('yes') # Flag params need special treatment
            if kwargs.get('verbose'): # Count params need special treatment
                self.verbose = self.verbose + kwargs.get('verbose')
            set_parameters(kwargs, all_yes=self.all_yes)
            logger.debug(json.dumps(get_parameters()))
            if not self._logger:
                self._logger = set_cid_logger(
                    verbosity=self.verbose,
                    log_filename=get_parameters().get('log_filename', 'cid.log')
                )
                logger.info(f'Initializing CID {__version__} for {func.__name__}')
            if not self.base:
                self.aws_login()
            self.load_resources()
            return func(self, *args, **kwargs)
        return wrap

    def __loadPlugins(self) -> dict:
        try:
            _entry_points = entry_points().get('cid.plugins')
        except: # fallback for python version more than 3.7.x AND still less then 3.8
            _entry_points = [ep for ep in entry_points() if ep.group == 'cid.plugins']

        plugins = dict()
        print('Loading plugins...')
        logger.info(f'Located {len(_entry_points)} plugin(s)')
        for ep in _entry_points:
            if ep.value in plugins.keys():
                logger.info(f'Plugin {ep.value} already loaded, skipping')
                continue
            logger.info(f'Loading plugin: {ep.name} ({ep.value})')
            plugin = Plugin(ep.value)
            print(f"\t{ep.name} loaded")
            plugins.update({ep.value: plugin})
            try:
                resources = plugin.provides()
                if ep.value != 'cid.builtin.core':
                    resources.get('views', {}).pop('account_map', None) # protect account_map from overriding
                self.resources = merge_objects(self.resources, resources, depth=1)
            except AttributeError:
                logger.warning(f'Failed to load {ep.name}')
        print('\n')
        logger.info('Finished loading plugins')
        return plugins

    def resources_with_global_parameters(self, resources):
        """ render resources with global parameters """
        params = self.get_template_parameters(self.resources.get('parameters', {}))
        def _recursively_process_strings(item, str_func):
            """ recursively update elements of a dict """
            if isinstance(item, str):
                return str_func(item)
            elif isinstance(item, dict):
                res = {}
                for key, value in item.items():
                    res[_recursively_process_strings(key, str_func)] = _recursively_process_strings(value, str_func)
                return res
            elif isinstance(item, list):
                return [_recursively_process_strings(value, str_func) for value in item]
            return item
        def _str_func(text):
            return Template(text).safe_substitute(params)
        return _recursively_process_strings(resources, _str_func)


    def getPlugin(self, plugin) -> dict:
        return self.plugins.get(plugin)


    def get_definition(self, type: str, name: str=None, id: str=None, noparams: bool=False) -> dict:
        """ return resource definition that matches parameters 
        :noparams: do not process parameters as they may not exist by this time
        """
        res = None
        if type not in ['dashboard', 'dataset', 'view', 'schedule', 'crawler', 'agent', 'space']:
            raise ValueError(f'{type} is not a valid definition type')
        if type in ['dataset', 'view', 'schedule', 'crawler', 'agent', 'space'] and name:
            res = self.resources.get(f'{type}s').get(name)
        elif type in ['dashboard']:
            for definition in self.resources.get(f'{type}s').values():
                if name is not None and definition.get('name') != name:
                    continue
                if id is not None and definition.get('dashboardId') != id:
                    continue
                res = definition
                break

        # template
        if isinstance(res, dict) and not noparams:
            name = name or res.get('name')
            params = self.get_template_parameters(res.get('parameters', {}), param_prefix=f'{type}-{name}-')
            # FIXME: can be recursive?
            for key, value in res.items():
                if isinstance(value, str):
                    res[key] = Template(value).safe_substitute(params)
                    if type in ['agent', 'space']:
                        for match in Template.pattern.finditer(res[key]):
                            token = match.group('named') or match.group('braced')
                            if token:
                                raise ValueError(f'Unresolved token ${{{token}}} in {type} definition {name!r}')
        return res


    @command
    def export(self, **kwargs):
        export_analysis(self.qs, self.athena, glue=self.glue)

    def track(self, action, dashboard_id):
        """ Send dashboard_id and account_id to CID adoption tracker """
        method = {'created':'PUT', 'updated':'PATCH', 'deleted': 'DELETE'}.get(action, None)
        if not method:
            logger.debug(f"This will not fail the deployment. Logging action {action} is not supported. This issue will be ignored")
            return
        endpoint = 'https://okakvoavfg.execute-api.eu-west-1.amazonaws.com/' # AWS Managed
        if os.environ.get('AWS_DEPLOYMENT_TYPE'):
            deployment_type = os.environ.get('AWS_DEPLOYMENT_TYPE')
        elif os.environ.get('AWS_EXECUTION_ENV', '').startswith('AWS_Lambda'):
            deployment_type = 'Lambda'
        else:
            deployment_type = 'CID'
        payload = {
            'dashboard_id': dashboard_id,
            'account_id': self.base.account_id,
            action + '_via': deployment_type,
        }
        try:
            res = requests.request(
                method=method,
                url=endpoint,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            if res.status_code != 200:
                logger.debug(f"This will not fail the deployment. There has been an issue logging action {action}  for dashboard {dashboard_id} and account {self.base.account_id}, server did not respond with a 200 response,actual  status: {res.status_code}, response data {res.text}. This issue will be ignored")
        except Exception as e:
            logger.debug(f"Issue logging action {action}  for dashboard {dashboard_id} , due to a urllib3 exception {str(e)} . This issue will be ignored")

    def get_page(self, source):
        resp = requests.get(source, timeout=10, headers={'User-Agent': 'cid'})
        resp.raise_for_status()
        return resp

    def load_resources(self):
        ''' load additional resources from command line parameters
        '''
        if get_parameters().get('catalog'):
            self.catalog_urls = get_parameters().get('catalog').split(',')
        for catalog_url in self.catalog_urls:
            self.load_catalog(catalog_url)
        if get_parameters().get('resources'):
            source = get_parameters().get('resources')
            self.load_resource_file(source, os.getcwd())
        self.resources = self.resources_with_global_parameters(self.resources)

    def resolve_relative_path(self, source, parent_source=None):
        if not source.startswith('https://'): # it is a relative path
            if not parent_source:
                parent_source = os.getcwd()
                logger.error(f'Parent not provided to get {source}. trying current folder {parent_source}')
            if parent_source.startswith('https://'):
                source = urllib.parse.urljoin(parent_source, source)
            else: # it is a local file, so expand that
                parent_source = os.path.abspath(os.path.expanduser(parent_source))
                if os.path.isfile(parent_source):
                    parent_source = os.path.dirname(parent_source)
                source = os.path.abspath(os.path.join(parent_source, source))
                if not os.path.isfile(source):
                    raise CidCritical(f'Cannot find {source} file')
        return source


    def load_text_file(self, source, parent_source=None):
        ''' return a text from local or remote file
        '''
        source = self.resolve_relative_path(source, parent_source)
        if source.startswith('https://'):
            return self.get_page(source).text
        else:
            with open(source, encoding='utf-8') as file_:
                return file_.read()


    def load_resource_file(self, source, parent_source=None):
        ''' load additional resources from resource file
        '''
        logger.debug(f'Loading resources from {source} from {parent_source}')
        resources = {}
        try:
            text = self.load_text_file(source, parent_source)
            resources = yaml.safe_load(text)
        except Exception as exc:
            logger.warning(f'Failed to load resources from {source}: {exc}')
            return
        resources.get('views', {}).pop('account_map', None) # Exclude account map as it is a special view
        for groups_of_resources in resources.values(): # add source metadata to each loaded resource
            if isinstance(groups_of_resources, dict):
                for res in groups_of_resources.values():
                    res['source'] = self.resolve_relative_path(source, parent_source)
        self.resources = merge_objects(self.resources, resources, depth=1)

    def load_catalog(self, catalog_url):
        ''' load additional resources from catalog
        '''
        try:
            text = self.load_text_file(catalog_url, os.getcwd())
            catalog = yaml.safe_load(text)
        except (requests.exceptions.RequestException, yaml.error.MarkedYAMLError) as exc:
            logger.warning(f'Failed to load a catalog url: {exc}')
            logger.debug(exc, exc_info=True)
            return
        for resource_ref in catalog.get('Resources', []):
            self.load_resource_file(resource_ref.get("Url"), catalog_url)


    def get_template_parameters(self, parameters: dict, param_prefix: str='', others: dict=None):
        """ Get template parameters. """
        params = get_parameters()
        others = others or {}
        for key, value in parameters.items():
            logger.debug(f'reading template parameter: {key} / {value}')
            prefix = '' if value.get('global') else param_prefix
            if isinstance(value, str):
                params[key] = value
            elif isinstance(value, dict) and value.get('type') == 'cur.tag_and_cost_category_fields':
                params[key] = get_parameter(
                    param_name=prefix + key,
                    message=f"Required parameter: {key} ({value.get('description')})",
                    choices=self.cur.tag_and_cost_category_fields + ["'none'"],
                )
            elif isinstance(value, dict) and value.get('type') == 'tags_json': # a json
                # Reuse already rendered SQL from a previous resolution
                cache_key = f'_tags_json_sql_{prefix}{key}'
                if hasattr(self, cache_key):
                    params[key] = getattr(self, cache_key)
                elif get_parameters().get((prefix + key).replace('_', '-')): # priority to user input
                    params[key] = get_parameters().get((prefix + key).replace('_', '-'))
                    if isinstance(params[key], str):
                        params[key] = params[key].split(',')
                elif not utils.isatty():
                    params[key] = "'{}'"
                else:
                    if 'query' not in value:
                        raise CidCritical(f'Failed fetching parameter {prefix}{key}: parameter with type Athena must have query value.')
                    query = Template(value['query']).safe_substitute(params|others)
                    try:
                        res_list = self.athena.query(query)
                    except (self.athena.client.exceptions.ClientError, CidError, CidCritical) as exc:
                        raise CidCritical(f'Failed fetching parameter {prefix}{key}: {exc}.') from exc
                    options = ['-'.join(res) for res in (res_list or [])]
                    params[key] = self.generic_tags_json(
                        param_name=key,
                        options=options,
                    )
                # Cache rendered SQL for reuse by subsequent views
                if isinstance(params.get(key), str):
                    setattr(self, cache_key, params[key])
            elif isinstance(value, dict) and value.get('type') == 'athena':
                if get_parameters().get(prefix + key): # priority to user input
                    params[key] = get_parameters().get(prefix + key)
                else:
                    if 'query' not in value:
                        raise CidCritical(f'Failed fetching parameter {prefix}{key}: parameter with type Athena must have query value.')
                    query = value['query']
                    try:
                        res_list = self.athena.query(query)
                    except (self.athena.client.exceptions.ClientError, CidError, CidCritical) as exc:
                        raise CidCritical(f'Failed fetching parameter {prefix}{key}: {exc}.') from exc
                    if not res_list:
                        raise CidCritical(f'Failed fetching parameter {prefix}{key}, {value}. Athena returns empty results. {value.get("error", "")}')
                    options = ['-'.join(res) for res in res_list]
                    default = value.get('default', '{default not provided}')
                    if len(options) == 1:
                        # silently taking the 1st available option
                        params[key] = options[0]
                    elif not utils.isatty():
                        if default in options:
                            # silently taking the default option if we cannot ask user
                            params[key] = default
                        else:
                            # silently taking the first option
                            params[key] = sorted(options)[0]
                    else:
                        # asking user
                        params[key] = get_parameter(
                            param_name=prefix + key,
                            message=f"Required parameter: {key} ({value.get('description')})",
                            choices=options,
                            default=default if default in options else None,
                        )
            elif isinstance(value, dict):
                params[key] = value.get('value')
                while params[key] is None:
                    if value.get('silentDefault') is not None and get_parameters().get(key) is None:
                        params[key] = value.get('silentDefault')
                    else:
                        params[key] = get_parameter(
                            param_name=prefix + key,
                            message=f"Required parameter: {key} ({value.get('description')})",
                            default=value.get('default'),
                            template_variables=dict(account_id=self.base.account_id),
                        )
            else:
                raise CidCritical(f'Unknown parameter type for "{key}". Must be a string or a dict with value or with default key')
        return merge_objects(params, others or {}, depth=1)


    @command
    def deploy(self, dashboard_id: str=None, recursive=True, update=False, **kwargs):
        """ Deploy Dashboard Command"""
        self._deploy(dashboard_id, recursive, update, **kwargs)


    def load_default_parameters(self):
        defaults = self.parameters_controller.load_parameters(
            context=get_parameters().get('dashboard-id')
        )
        if defaults:
            logger.debug(f'loaded default from Athena {defaults}')
            set_defaults(defaults)

    def dump_default_parameters(self):
        stop_list = ['profile-name', 'region', 'aws-access-key-id', 'aws-secret-access-key', 'aws-session-token', 'athena-database', 'athena-workgroup']
        current_defaults = get_parameters()
        for key in list(current_defaults.keys()):
            if key in stop_list:
                del current_defaults[key]
        logger.trace(f'dumping parameters {current_defaults}')
        self.parameters_controller.dump_parameters(
            current_defaults,
            context=get_parameters().get('dashboard-id')
        )


    def ensure_subscription(self):
        for _ in range(3):
            try:
                return self.qs.ensure_subscription()
            except CidCritical as exc:
                if 'QuickSight is not activated' in str(exc):
                    self.init_qs()
                    unset_parameter('enable-quicksight-enterprise') # in case if customer answered no
                else:
                    raise
        else:
            raise CidCritical('QuickSight is not activated. Please open https://quicksight.aws.amazon.com/ and activate ENTERPRISE subscription.')

    def _deploy(self, dashboard_id: str=None, recursive=True, update=False, **kwargs):
        """ Deploy Dashboard """

        self.ensure_subscription()

        self.qs.pre_discover()

        dashboard_id = dashboard_id or get_parameters().get('dashboard-id')
        if dashboard_id and not get_parameters().get('dashboard-id'):
            set_parameters({'dashboard-id': dashboard_id})
        category_filter = [cat for cat in get_parameters().get('category', '').upper().split(',') if cat]
        if not dashboard_id:
            standard_categories = ['Foundational', 'Advanced', 'Additional'] # Show these categories first
            all_categories = set([f"{dashboard.get('category', 'Other')}" for dashboard in self.resources.get('dashboards').values()])
            non_standard_categories = [cat for cat in all_categories if cat not in standard_categories]
            categories =  standard_categories + sorted(non_standard_categories)
            dashboard_options = {}
            for category in categories:
                if category_filter and category.upper() not in category_filter:
                    continue
                dashboard_options[f'{category.upper()}'] = '[category]'
                counter = 0
                for dashboard in self.resources.get('dashboards').values():
                    if dashboard.get('deprecationNotice'):
                        continue
                    if dashboard.get('category', 'Other') == category:
                        check = '✓' if dashboard.get('dashboardId') in self.qs.dashboards else ' '
                        dashboard_options[f" {check}[{dashboard.get('dashboardId')}] {dashboard.get('name')}"] = dashboard.get('dashboardId')
                        counter += 1
                if not counter: # remove empty categories
                    del dashboard_options[f'{category.upper()}']
            while True:
                dashboard_id = get_parameter(
                    param_name='dashboard-id',
                    message="Please select a dashboard to deploy",
                    choices=dashboard_options,
                )
                if dashboard_id == '[category]':
                    unset_parameter('dashboard-id')
                    continue
                break

        if not dashboard_id:
            print('No dashboard selected')
            return

        self.load_default_parameters()

        # Get selected dashboard definition
        dashboard_definition = self.get_definition("dashboard", id=dashboard_id)
        dashboard = None
        try:
            dashboard = self.qs.discover_dashboard(dashboard_id)
        except CidCritical:
            pass

        if not dashboard_definition:
            if isinstance(dashboard, Dashboard):
                dashboard_definition = dashboard.definition
            else:
                raise ValueError(f'Cannot find dashboard with id={dashboard_id} in resources file.')

        definition_dependency_datasets = dashboard_definition.get('dependsOn', {}).get('datasets', [])
        required_datasets_names = [dsname for dsname in definition_dependency_datasets]
        ds_map = definition_dependency_datasets if isinstance(definition_dependency_datasets, dict) else {}

        dashboard_datasets = dashboard.datasets if dashboard else {}

        for name, id in dashboard_datasets.items():
            if id not in self.qs.datasets:
                logger.info(f'Removing unknown dataset "{name}" ({id}) from dashboard {dashboard_id}')
                del dashboard_datasets[name]

        if dashboard_definition.get('templateId'):
            # Get QuickSight template details
            try:
                source_template = self.qs.describe_template(
                    template_id=dashboard_definition.get('templateId'),
                    account_id=dashboard_definition.get('sourceAccountId'),
                    region=dashboard_definition.get('region', 'us-east-1')
                )
            except CidError as exc:
                raise CidCritical(exc) # Cannot proceed without a valid template
            dashboard_definition['sourceTemplate'] = source_template
            print(f'\nLatest template: {source_template.arn}/version/{source_template.version}')
        elif dashboard_definition.get('data') or dashboard_definition.get('file') or dashboard_definition.get('url'):
            data = self.get_data_from_definition(dashboard_definition)
            if isinstance(data, dict):
                data = yaml.safe_dump(data, width=100000) # dump without line breaks
            params = self.get_template_parameters(dashboard_definition.get('parameters', dict()))
            data = Template(data).safe_substitute(params)
            dashboard_definition['definition'] = yaml.safe_load(data)
        else:
            raise CidCritical('Definition of dashboard resource must contain data or template_id')

        compatible = self.check_dashboard_version_compatibility(dashboard_id)
        if not recursive and compatible == False:
            if not get_yesno_parameter(
                param_name=f'confirm-recursive',
                message=f'This is a major update and require recursive action. This could lead to the loss of dataset customization. Continue anyway?',
                default='yes'):
                return
            logger.info("Switch to recursive mode")
            recursive = True

        logger.debug(f'found  dashboard_datasets= {dashboard_datasets}')

        if recursive:
            logger.info('creating datasets')
            dashboard_datasets = self.create_datasets(required_datasets_names, known_datasets=dashboard_datasets, recursive=recursive, update=update)

        # Find datasets for template or definition
        if not dashboard_definition.get('datasets'):
            dashboard_definition['datasets'] = {}

        logger.debug(f'found  dashboard_datasets= {dashboard_datasets}')

        for dataset_name in required_datasets_names:
            dataset = None
            # First try existing datasets
            if dashboard_datasets.get(dataset_name):
                dataset = self.qs.describe_dataset(id=dashboard_datasets.get(dataset_name), no_cache=True)

            if not isinstance(dataset, Dataset):
                # Second chance:  try to find the dataset with the id from resources
                try:
                    _ds_id = self.resources['datasets'].get(dataset_name, {}).get('data', {}).get('DataSetId')
                    if _ds_id:
                        dataset = self.qs.describe_dataset(id=_ds_id, no_cache=True)
                except Exception as exc:
                    logger.debug(f'Failed to describe_dataset {dataset_name} {exc}')

            if not isinstance(dataset, Dataset):
                # Third chance:  try to find the dataset with the id that is the name
                try:
                    dataset = self.qs.describe_dataset(id=dataset_name, no_cache=True)
                except Exception as exc:
                    logger.debug(f'Failed to describe_dataset {dataset_name} {exc}')

            if isinstance(dataset, Dataset):
                logger.debug(f'Found dataset {dataset_name} with id match = {dataset.arn}')
                dashboard_definition['datasets'][dataset_name] = dataset.arn

            else:
                # Then search dataset by name.
                # This is not ideal as there can be several with the same name,
                # but if dataset is created manually we cannot use id.
                matching_datasets = []
                for ds in self.qs.datasets.values():
                    if not isinstance(ds, Dataset) or ds.name != dataset_name:
                        continue
                    if dashboard_definition.get('templateId'):
                        # For templates we can additionally verify dataset fields
                        dataset_fields = {col.get('Name'): col.get('Type') for col in ds.columns}
                        src_fields = source_template.datasets.get(ds_map.get(dataset_name, dataset_name) )
                        required_fields = {col.get('Name'): col.get('DataType') for col in src_fields}
                        unmatched = {}
                        for field_name, field_type in required_fields.items():
                            if field_name not in dataset_fields or dataset_fields[field_name] != field_type:
                                unmatched.update({field_name: {'expected': field_type, 'found': dataset_fields.get(field_name)}})
                        logger.debug(f'unmatched_fields={unmatched}')
                        if unmatched:
                            logger.warning(f'Found Dataset "{dataset_name}" ({ds.id}) but it is missing required fields. {(unmatched)}')
                        else:
                            matching_datasets.append(ds)
                    else:
                        # for definitions datasets we do not have any possibility to check if dataset with a given name matches
                        matching_datasets.append(ds)

                if not matching_datasets:
                    reco = ''
                    new_datasets = self.create_datasets([dataset_name], known_datasets=dashboard_datasets, recursive=recursive, update=update)
                    if not new_datasets:
                        logger.warning(f'Dataset {dataset_name} is not found.')
                        if utils.exec_env()['shell'] == 'lambda':
                            # We are in lambda
                            reco = 'You can try deleting existing dataset and re-run.'
                        else:
                            # We are in command line mode
                            reco = 'Please retry with --update "yes" --force --recursive flags.'
                        raise CidCritical(f'Failed to find a Dataset "{dataset_name}" with required fields. ' + reco)
                    _ds_id = self.resources['datasets'].get(dataset_name, {}).get('data', {}).get('DataSetId')
                    dashboard_definition['datasets'][dataset_name] = f'arn:{self.base.partition}:quicksight:{self.base.region}:{self.base.account_id}:dataset/{_ds_id}'
                elif len(matching_datasets) >= 1:
                    if len(matching_datasets) > 1:
                        # FIXME: propose a choice?
                        logger.warning(
                            f'Found {len(matching_datasets)} Datasets found with name "{dataset_name}":'
                            f' {str([ds.id for ds in matching_datasets])}'
                        )
                    ds = matching_datasets[0]
                    print(f'Using dataset {dataset_name}: {ds.id}')
                    dashboard_definition['datasets'][dataset_name] = ds.arn

        # Update datasets to the mapping name if needed
        # Dashboard definition must contain names that are specific to template.
        dashboard_definition['datasets'] = {ds_map.get(name, name): arn for name, arn in dashboard_definition['datasets'].items() }
        logger.debug(f"datasets: {dashboard_definition['datasets']}")

        _url = self.qs_url.format(dashboard_id=dashboard_id, **self.qs_url_params)

        dashboard = self.qs.describe_dashboard(DashboardId=dashboard_id)
        if isinstance(dashboard, Dashboard):
            if update:
                return self.update_dashboard(dashboard_id, dashboard_definition)
            else:
                print(f'Dashboard {dashboard_id} exists. See {_url}')
                return dashboard_id

        print(f'Deploying dashboard {dashboard_id}')
        try:
            dashboard = self.qs.create_dashboard(dashboard_definition)
            print(f"\n#######\n####### Congratulations!\n####### {dashboard_definition.get('name')} is available at: {_url}\n#######")
            self.track('created', dashboard_id)
        except self.qs.client.exceptions.ResourceExistsException:
            print('error, already exists')
            print(f"#######\n####### {dashboard_definition.get('name')} is available at: {_url}\n#######")
        except Exception as e:
            # Catch exception and dump a reason
            logger.debug(e, exc_info=True)
            print(f'failed with an error message: {e}')
            self.delete(dashboard_id)
            raise CidCritical(f'Deploy failed: {e}')

        if get_yesno_parameter(
                param_name=f'share-with-account',
                message=f'Share this dashboard with everyone in the account?',
                default='yes'):
            set_parameters({'share-method': 'account'})
            self.share(dashboard_id)

        self.dump_default_parameters()
        return dashboard_id


    # --- Quick Suite Agent platform: create-agent (prerequisite-only, never deploys) ---

    # Deployment guidance pointers.
    # These messages MUST NOT name any cid-cmd command.
    CID_DASHBOARDS_GUIDANCE = (
        'the CID dashboards deployment guide '
        '(https://catalog.workshops.aws/awscid/en-US)'
    )
    DATA_COLLECTION_GUIDANCE = (
        'the Data Collection deployment guide '
        '(https://catalog.workshops.aws/awscid/en-US/data-collection)'
    )
    # QuickSight user roles entitled to create Quick Agents
    AGENT_AUTHOR_PRO_ROLES = ('AUTHOR_PRO', 'ADMIN_PRO')

    @command
    def create_agent(self, agent_id: str=None, space_name: str=None, **kwargs):
        """Create a Quick Agent and its knowledge Space over already-deployed dashboards.

        Prerequisite-only flow: never deploys a dashboard, never creates
        a dataset, and never touches the data layer (CUR, Data Exports, Data Collection).
        Missing prerequisites are resolved by guidance, not by deployment.

        When the agent is already deployed, the command shows what differs from the
        catalog and asks whether to update instead (``--update yes`` to skip the
        prompt; ``-y`` implies yes).
        """
        return self._agent_apply_flow(agent_id=agent_id, space_name=space_name, mode='create')

    @command
    def update_agent(self, agent_id: str=None, space_name: str=None, **kwargs):
        """Update a deployed CID-managed Quick Agent to match the catalog.

        Requires the agent to exist (run create-agent first). Shows which
        catalog-managed fields drifted and asks for confirmation before
        overriding them. Space associations are reconciled additively by
        default; ``--sync-spaces`` opts into exact synchronization.
        """
        return self._agent_apply_flow(agent_id=agent_id, space_name=space_name, mode='update')

    def _agent_apply_flow(self, agent_id: str=None, space_name: str=None, mode: str='create'):
        """Shared create-agent / update-agent flow; ``mode`` gates the existing-agent branch."""
        # 0. gen-AI availability pre-check: create nothing when unavailable
        self._check_genai_availability()
        # 1. DETECT-AND-REQUIRE Enterprise subscription, read-only
        self._verify_enterprise_subscription()
        # 2-3. resolve the agent id (picker when absent) and its definition
        agent_key, definition = self._resolve_agent_definition(agent_id)
        # 4. stored default parameters keyed by the agent id
        self._load_agent_default_parameters(agent_key)
        # 5. cap validation before any CreateAgent call
        validate_caps(definition)
        # 6. owner principal: registered QuickSight user with Author Pro
        principal_arn = self._resolve_quicksight_principal()

        # 6b. existing-agent gate — BEFORE any create or modify call.
        target_agent_id = definition['agentId']
        existing_agent = self.agent.get(target_agent_id)
        if existing_agent is not None and not self._agent_is_cid_managed(existing_agent):
            raise CidError(
                f'An agent with id {target_agent_id!r} already exists but was not created by '
                'Cloud Intelligence Dashboards. Refusing to modify it. Please rename the catalog '
                'agent id or remove the existing agent, then re-run.'
            )
        drift = describe_drift(definition, existing_agent) if existing_agent is not None else []
        if mode == 'create' and existing_agent is not None:
            drift_note = (f'It differs from the catalog in: <BOLD>{", ".join(drift)}<END>.'
                          if drift else 'Its managed fields match the catalog.')
            cid_print(f'Agent <BOLD>{target_agent_id}<END> is already deployed. {drift_note}')
            if not isatty() and not getattr(self, 'all_yes', False) and get_parameters().get('update') is None:
                raise CidError(
                    f'Agent {target_agent_id!r} is already deployed. Pass --update yes to update it '
                    f'to the catalog, or run cid-cmd update-agent --agent-id {agent_key}.'
                )
            if not get_yesno_parameter(
                    param_name='update',
                    message=f'Update agent {target_agent_id} to match the catalog? '
                            'This overrides console customizations to the managed fields',
                    default='no'):
                cid_print(f'No changes made. Run <BOLD>cid-cmd update-agent --agent-id {agent_key}<END> when ready.')
                return target_agent_id
        if mode == 'update':
            if existing_agent is None:
                raise CidError(
                    f'Agent {target_agent_id!r} is not deployed, so there is nothing to update. '
                    f'Run cid-cmd create-agent --agent-id {agent_key} first.'
                )
            if drift:
                cid_print(f'Agent <BOLD>{target_agent_id}<END> differs from the catalog in: '
                          f'<BOLD>{", ".join(drift)}<END>.')
                if not isatty() and not getattr(self, 'all_yes', False) and get_parameters().get('confirm-update') is None:
                    # unattended update-agent: the user asked for the update explicitly
                    logger.info('Unattended mode: applying the update without a prompt.')
                elif not get_yesno_parameter(
                        param_name='confirm-update',
                        message=f'Update agent {target_agent_id}? '
                                'This overrides console customizations to these fields',
                        default='yes'):
                    cid_print('No changes made.')
                    return target_agent_id

        # 7. read-only dependency pre-flight — NEVER deploys
        dependencies = self._preflight_agent_dependencies(definition)

        space_name = space_name or get_parameters().get('space-name') or get_parameters().get('space')
        remove_stale = bool(get_parameters().get('cleanup-space'))
        repair = bool(get_parameters().get('repair'))
        sync_spaces = bool(get_parameters().get('sync-spaces'))
        access_denied = self.space.client.exceptions.AccessDeniedException
        verb = 'Updating' if mode == 'update' else 'Creating'
        cid_print(f"\n{verb} agent <BOLD>{definition.get('name') or target_agent_id}<END>...")
        try:
            # 8. resolve the target Space
            space_id, space_arn = self._resolve_agent_space(definition, space_name, principal_arn)
            # 9. add missing resources for the PRESENT dashboards only
            managed_arns = None
            if remove_stale:  # opt-in, scoped stale removal
                deployed_by_id = dependencies['deployed_dashboard_arns_by_id']
                managed_arns = (
                    self._cid_managed_dashboard_arns(deployed_by_id)
                    - self._arns_referenced_by_any_agent(deployed_by_id)
                )
            self.space.update_resources(
                space_id, dependencies['dashboard_arns'],
                resource_type='DASHBOARD', remove_stale=remove_stale, managed_arns=managed_arns,
            )
            # dataset knowledge: attach only PRESENT dataset ARNs, never create
            if dependencies['dataset_arns']:
                self.space.update_resources(space_id, dependencies['dataset_arns'], resource_type='DATA_SET')
            dashboards_count = len(dependencies['dashboard_arns'])
            datasets_count = len(dependencies['dataset_arns'])
            knowledge = f"{dashboards_count} dashboard{'s' if dashboards_count != 1 else ''}"
            if datasets_count:
                knowledge += f", {datasets_count} dataset{'s' if datasets_count != 1 else ''}"
            cid_print(f'\tKnowledge: {knowledge} linked')
            # 10. optional pre-existing knowledge bases; never create a KB
            self._attach_knowledge_bases(space_id, dependencies['knowledge_base_arns'])
            # 11. create-or-update (existence and provenance were gated in step 6b)
            try:
                result = self.agent.create_or_update(definition, [space_arn], sync_spaces=sync_spaces)
            except access_denied:
                raise  # no further create/modify (incl. cleanup) after AccessDenied
            except (CidError, CidCritical):
                raise
            except Exception:
                if existing_agent is None:
                    # cleanup-on-failure: best-effort delete of the partially created agent,
                    # then re-raise; delete retries through ConflictException
                    try:
                        self.agent.delete(target_agent_id)
                    except Exception as cleanup_exc:
                        logger.warning(f'Cleanup of partially created agent {target_agent_id!r} failed: {cleanup_exc}')
                raise
            if result.get('action') == 'created' and result.get('arn'):
                # dual provenance write: tag (tolerated failure) + Description marker
                self.agent.write_provenance(result['arn'], target_agent_id)
            action_word = {'created': 'created', 'updated': 'updated', 'unchanged': 'up to date'}.get(result.get('action'), 'ready')
            cid_print(f'\tAgent: {action_word} ({target_agent_id})')
            # --repair (update-agent): detach and re-attach the Space links so the
            # service rewrites them. Recovers an agent whose Space shows as
            # unavailable although it describes as healthy.
            if repair and mode == 'update':
                if self.agent.repair_space_associations(target_agent_id):
                    cid_print(f'Space links of agent <BOLD>{target_agent_id}<END> rewritten (detach + re-attach).')
            # 12. owner grant: the 5-action bundle as one set
            self.agent.grant_owner(target_agent_id, principal_arn)
            # 13. PUBLISHED lifecycle: wait for ACTIVE; fail fast on FAILED
            if str(definition.get('lifecycle') or '').upper() == 'PUBLISHED':
                changed = result.get('action') != 'unchanged'
                if changed:
                    cid_print('\tStatus: waiting to become ACTIVE')
                wait_started = time.time()
                self.agent.wait_active(target_agent_id)
                cid_print(f'\tStatus: ACTIVE ({int(time.time() - wait_started)}s)' if changed
                          else '\tStatus: ACTIVE')
        except access_denied as exc:
            operation = getattr(exc, 'operation_name', None) or 'a Quick Suite generative-AI operation'
            raise CidCritical(
                f'Access denied calling {operation}. The caller is missing the QuickSight permission '
                f'quicksight:{operation}. No further create or modify calls were made. Please grant the '
                'missing permission and re-run.'
            ) from exc

        # 14. persist parameters keyed by the agent id + print the console URL
        self._dump_agent_default_parameters(agent_key)
        console_url = build_console_url(
            self.base.account_id, self.base.region, self.base.partition, self.base.domain,
            'agent', target_agent_id,
        )
        action = {'created': 'created', 'updated': 'updated', 'unchanged': 'up to date'}.get(result.get('action'), 'ready')
        cid_print(
            f"\n#######\n####### Congratulations!\n"
            f"####### Agent '{definition.get('name')}' ({target_agent_id}) is {action} "
            f"and available at: {console_url}\n#######\n"
        )
        return target_agent_id

    def _check_genai_availability(self):
        """Verify the Quick Suite gen-AI operations are available before creating anything."""
        mapping = getattr(self.space.client.meta, 'method_to_api_mapping', None) or {}
        if 'create_space' not in mapping or 'create_agent' not in mapping:
            raise CidCritical(
                'The Quick Suite generative-AI operations (CreateSpace/CreateAgent) are not available '
                f'in region {self.base.region!r} of partition {self.base.partition!r} with the installed '
                'AWS SDK. If this region and partition support Quick Suite, please upgrade the AWS SDK '
                '(botocore >= 1.43) so the QuickSight client carries the gen-AI API models. '
                'No Quick resource was created.'
            )

    def _verify_enterprise_subscription(self):
        """Read-only DETECT-AND-REQUIRE of QuickSight Enterprise.

        NEVER activates, enables, or modifies a subscription (unlike Cid.ensure_subscription,
        which offers activation) — billing is the customer's explicit action.
        """
        try:
            self.qs.ensure_subscription()
        except CidCritical as exc:
            raise CidCritical(
                'An active Amazon QuickSight Enterprise subscription is required to create Quick '
                'Agents, and this command will not activate one. Please enable the QuickSight '
                'Enterprise edition from the QuickSight console (https://quicksight.aws.amazon.com/) '
                f'and re-run. Detection result: {exc}'
            ) from exc

    def _deployed_agent_ids(self) -> set:
        """Enumerate deployed agent ids via read-only ListAgents (enumeration only).

        ListAgents omits PREVIEW/FAILED agents, so this is used solely for the picker's
        check indicator; existence checks use DescribeAgent. Failures are
        tolerated: the picker then simply shows no check indicators.
        """
        ids = set()
        try:
            parameters = {'AwsAccountId': self.base.account_id}
            while True:
                response = self.agent.client.list_agents(**parameters)
                summaries = response.get('AgentSummaries') or response.get('agentSummaries') or []
                for summary in summaries:
                    an_id = summary.get('AgentId') or summary.get('agentId')
                    if an_id:
                        ids.add(an_id)
                next_token = response.get('NextToken') or response.get('nextToken')
                if not next_token:
                    break
                parameters['NextToken'] = next_token
        except Exception as exc:
            logger.debug(f'ListAgents failed ({exc}). Deployed check indicators will be skipped.')
        return ids

    def _agent_picker_options(self, listing: dict, agents_catalog: dict=None,
                              deployed_dashboard_arns: dict=None) -> dict:
        """Build the agent picker choices from a build_agent_listing result.

        Name-first rows grouped under non-selectable category dividers of a
        uniform width. When ``deployed_dashboard_arns`` is provided, each agent
        row is followed by a non-selectable dependency annotation block. The
        deployment badge is right-aligned to the divider edge so agent state
        never blends into the per-dashboard marks::

            ── Foundational ──────────────────────────────────────────
            CID FinOps Advisor                            [DEPLOYED]
                └─ required: ✓ CUDOSv5  ✓ CID  ✓ KPI    optional: ✗ Trends

        Display names lead; the agent id is appended only when two catalog
        entries share a display name (labels must stay unique).
        """
        divider_width = 62
        annotation_prefix = '    └─ '
        deployed_badge = '[DEPLOYED]'
        entries = [entry for group in listing.values() for entry in group]
        name_counts = {}
        for entry in entries:
            name_counts[entry['name']] = name_counts.get(entry['name'], 0) + 1
        annotated = deployed_dashboard_arns is not None and agents_catalog is not None
        if annotated:
            picker_width = min(shutil.get_terminal_size((120, 24)).columns - 4, 116)
        options = {}
        for category_position, (category, group) in enumerate(listing.items()):
            if category_position:  # blank rows separate categories and agents
                options[f'_gap_category_{category}'] = Separator(' ')
            divider = f'── {category} ' + '─' * max(3, divider_width - len(category) - 4)
            options[category] = Separator(divider)
            for position, entry in enumerate(group):
                if annotated and position:
                    options[f"_gap_{entry['key']}"] = Separator(' ')
                name = entry['name']
                if name_counts[name] > 1:  # disambiguate identical display names
                    name = f"{name} [{entry['agentId']}]"
                if entry['deployed']:  # badge right-aligned to the divider edge
                    padding = max(divider_width - len(deployed_badge) - len(name), 2)
                    label = name + ' ' * padding + deployed_badge
                else:
                    label = name
                options[label] = entry['key']
                if annotated:
                    definition = agents_catalog.get(entry['key']) or {}
                    dependency_lines = self._agent_dependency_lines(
                        definition, deployed_dashboard_arns,
                        color=False, indent=' ' * len(annotation_prefix), width=picker_width)
                    if dependency_lines:  # hang marker on the first line only
                        dependency_lines[0] = annotation_prefix + dependency_lines[0][len(annotation_prefix):]
                    for line_number, line in enumerate(dependency_lines):
                        options[f"_deps_{entry['key']}_{line_number}"] = Separator(line)
        return options

    def _resolve_agent_definition(self, agent_id: str=None) -> tuple:
        """Resolve the agent id (category-grouped picker when absent) and load its definition.

        Returns (agent_key, definition) where definition is a copy of the catalog entry
        with the persona loaded inline.
        """
        agents_catalog = self.resources.get('agents') or {}
        if not agents_catalog:
            raise CidError('No agents found in the catalog.')
        agent_key = agent_id or get_parameters().get('agent-id')
        if not agent_key:
            # category-grouped picker: DEPLOYED badge via ListAgents, hide Deprecated,
            # dependency annotations per agent
            listing = build_agent_listing(agents_catalog, self._deployed_agent_ids())
            agent_options = self._agent_picker_options(
                listing, agents_catalog, self._deployed_dashboard_arns_or_none())
            try:
                agent_key = get_parameter(
                    param_name='agent-id',
                    message='Please select an agent to create',
                    choices=agent_options,
                    fuzzy=False,
                )
            except Exception as exc:
                # non-interactive with no value/default: name the missing input
                raise CidCritical(
                    "Required input 'agent-id' has no supplied value, stored default, or "
                    'fallback in a non-interactive environment. Please provide --agent-id.'
                ) from exc
        # tolerate the deployed agentId as input in addition to the catalog key
        if agent_key not in agents_catalog:
            agent_key = next(
                (key for key, entry in agents_catalog.items() if (entry or {}).get('agentId') == agent_key),
                agent_key,
            )
        definition = self.get_definition('agent', name=agent_key)
        if not isinstance(definition, dict):
            raise CidError(
                f'Agent {agent_key!r} is not found in the catalog. '
                'Please check the agent id or provide the catalog that defines it.'
            )
        set_parameters({'agent-id': agent_key})
        definition = dict(definition)  # do not mutate the shared catalog entry
        definition['persona'] = self._load_agent_persona(definition)
        return agent_key, definition

    def _load_agent_persona(self, definition: dict) -> dict:
        """Load the persona for an agent definition (inline dict or resolved personaFile)."""
        persona = definition.get('persona')
        if isinstance(persona, dict):
            return persona
        persona_file = definition.get('personaFile')
        if not persona_file:
            return None  # validate_caps reports the missing required 'persona' field
        try:
            content = yaml.safe_load(self.load_text_file(persona_file, parent_source=definition.get('source')))
        except Exception as exc:
            raise CidError(f'Failed to load the persona file {persona_file!r}: {exc}') from exc
        if not isinstance(content, dict):
            raise CidError(f'Persona file {persona_file!r} must contain a mapping of the 5 persona fields.')
        return content

    def _agent_parameter_store_ready(self) -> bool:
        """Resolve the Athena database/workgroup for the agent Parameter_Store.

        The Parameter_Store (``parameters_controller`` over the ``cid_parameters`` view)
        queries Athena, and the Athena helper's ``DatabaseName``/``WorkGroup`` getters
        fall through to interactive prompts with CREATE NEW options when nothing is
        resolved. Agent commands are prerequisite-only and must never prompt for or
        create data-layer resources, so this guard resolves both non-interactively (or
        skips the store entirely). The outcome is computed at most once per command
        invocation and reused by every subsequent Parameter_Store load and persist
. The Athena helper itself and all non-agent commands keep their
        existing resolution behavior untouched.

        :returns: True when the Parameter_Store can be used without prompting;
            False when all load/persist operations must be skipped
        """
        if getattr(self, '_agent_pstore_ready', None) is None:
            self._agent_pstore_ready = self._resolve_agent_parameter_store()
        return self._agent_pstore_ready

    def _resolve_agent_parameter_store(self) -> bool:
        """Run the guarded, read-only resolution chain for the agent Parameter_Store.

        1. ``athena-database`` resolved in the session (parameter set, or the Athena
           helper's ``_DatabaseName`` already materialized) -> used unchanged.
        2. Otherwise read-only inference (no DDL, no create/modify): the well-known CID
           database names (``cid_cur``, ``cid_data_export``) present in the Athena
           catalog, else the majority database referenced by deployed CID datasets'
           physical table maps.
        3. The workgroup resolves non-interactively only: the explicit
           ``athena-workgroup`` parameter (or an already-materialized ``_WorkGroup``),
           else the Athena helper's default workgroup when it is usable as-is (exists,
           enabled, output location configured) — the interactive workgroup prompt is
           never displayed.

        Any failure (no candidate database, AccessDenied/client error on a lookup, or
        an unusable default workgroup) skips the Parameter_Store for the invocation
        with a single debug log; nothing is prompted for or created.
        """
        try:
            # session-resolved: parameter set, or _DatabaseName already materialized
            # (identity check only — never invokes anything on the Athena helper)
            database_resolved = bool(get_parameters().get('athena-database')) \
                or getattr(self.athena, '_DatabaseName', None) is not None
            if not database_resolved:
                database = self._infer_agent_athena_database()
                if not database:
                    raise CidError('no candidate Athena database was found in the account')
                # honored by the Athena helper's DatabaseName getter without any prompt
                set_parameters({'athena-database': database})
            workgroup_resolved = bool(get_parameters().get('athena-workgroup')) \
                or getattr(self.athena, '_WorkGroup', None) is not None
            if not workgroup_resolved:
                default_workgroup = str(self.athena.defaults.get('WorkGroup'))
                # read-only usability check: never create or reconfigure a workgroup
                workgroup = self.athena.client.get_work_group(WorkGroup=default_workgroup).get('WorkGroup', {})
                if workgroup.get('State') == 'DISABLED' \
                    or not workgroup.get('Configuration', {}).get('ResultConfiguration', {}).get('OutputLocation'):
                    raise CidError(f'the default Athena workgroup {default_workgroup!r} is not usable non-interactively')
                set_parameters({'athena-workgroup': default_workgroup})
            return True
        except Exception as exc:
            # the single per-invocation debug log of the skip
            logger.debug(
                f'Skipping the agent parameter store for this invocation: {exc}. '
                'The command completes normally without stored defaults.'
            )
            return False

    def _infer_agent_athena_database(self) -> str | None:
        """Infer the Athena database from the existing CID deployment, read-only.

        Candidates are evaluated in order: (a) the well-known CID database names
        (``cid_cur``, then ``cid_data_export``) when present in the Athena catalog;
        (b) the databases referenced by deployed CID datasets' physical table maps
        (``Dataset.schemas``), selected by majority with ascending lexicographic
        tie-break via :func:`select_database_from_candidates`. Issues only
        read-only API calls; lookup errors propagate to the caller's single-skip
        handling.
        """
        # (a) well-known CID database names, checked against the default catalog to avoid
        # the CatalogName getter (which can prompt when multiple catalogs exist)
        databases = self.athena.list_databases(catalog_name=self.athena.defaults.get('CatalogName'))
        for well_known in ('cid_cur', 'cid_data_export'):
            if well_known in databases:
                logger.debug(f'Inferred the Athena database {well_known!r} from the well-known CID database names.')
                return well_known
        # (b) databases referenced by deployed CID datasets' physical table maps
        candidates = []
        for dataset in (self.qs.datasets or {}).values():
            candidates += list(dataset.schemas or [])
        selected = select_database_from_candidates(candidates)
        if selected:
            logger.debug(
                f'Inferred the Athena database {selected!r} from deployed dataset physical table maps. '
                f'Competing candidates: {sorted(set(candidates))}.'
            )
        return selected

    def _load_agent_default_parameters(self, agent_key: str):
        """Load stored default parameters keyed by the agent id. Best-effort."""
        if not self._agent_parameter_store_ready():
            return  # skip silently, never prompt or create; the resolver logged the skip once
        try:
            defaults = self.parameters_controller.load_parameters(context=agent_key)
        except Exception as exc:
            logger.debug(f'Could not load stored default parameters for {agent_key!r}: {exc}')
            return
        if defaults:
            logger.debug(f'loaded defaults from the parameter store for {agent_key!r}: {defaults}')
            set_defaults(defaults)

    def _dump_agent_default_parameters(self, agent_key: str):
        """Persist the resolved parameters keyed by the agent id. Best-effort."""
        if not self._agent_parameter_store_ready():
            return  # skip silently, never prompt or create; the resolver logged the skip once
        stop_list = ['profile-name', 'region', 'aws-access-key-id', 'aws-secret-access-key', 'aws-session-token', 'athena-database', 'athena-workgroup']
        current_parameters = get_parameters()
        for key in list(current_parameters.keys()):
            if key in stop_list:
                del current_parameters[key]
        try:
            self.parameters_controller.dump_parameters(current_parameters, context=agent_key)
        except Exception as exc:
            logger.debug(f'Could not persist parameters for {agent_key!r}: {exc}')

    def _resolve_quicksight_principal(self) -> str:
        """Resolve the owner principal ARN before any grant.

        Resolution order: --quicksight-group, --quicksight-user, then the caller identity.
        The principal must be registered in QuickSight; user principals must
        additionally carry an Author Pro role to create Quick Agents ( — the role
        check is not applicable to groups).
        """
        group_name = get_parameters().get('quicksight-group')
        if group_name:
            group = self.qs.describe_group(group_name)
            if not group or not group.get('Arn'):
                raise CidCritical(
                    f'The QuickSight group {group_name!r} was not found in the default namespace. '
                    'Please create the group in QuickSight or provide --quicksight-user instead.'
                )
            return group['Arn']
        user_name = get_parameters().get('quicksight-user') or self.base.username
        try:
            user = self.qs.describe_user(user_name)
        except Exception as exc:
            logger.debug(exc, exc_info=True)
            user = None
        if not user or not user.get('Arn'):
            raise CidCritical(
                f'The principal {user_name!r} is not a registered QuickSight user, so ownership of the '
                'Quick Space and Agent cannot be granted. Please register it as a QuickSight user with '
                'the Author Pro role (RegisterUser), or pass --quicksight-user/--quicksight-group with '
                'a registered principal.'
            )
        role = user.get('Role')
        if role not in self.AGENT_AUTHOR_PRO_ROLES:
            raise CidCritical(
                f"The QuickSight user {user.get('UserName', user_name)!r} has role {role!r}, but creating "
                'Quick Agents requires the Author Pro entitlement. Please register or upgrade the user '
                'as an Author Pro (or Admin Pro) user and re-run.'
            )
        return user['Arn']

    def _collect_agent_dependency_keys(self, definition: dict) -> tuple:
        """Collect dependency keys from the agent manifest and every catalog Space it references.

        Returns (required_dashboards, optional_dashboards, datasets, knowledge_base_arns)
        preserving declaration order, de-duplicated.
        """
        required, optional, datasets, knowledge_bases = [], [], [], []
        depends_sources = [definition.get('dependsOn') or {}]
        for space_key in (definition.get('dependsOn') or {}).get('spaces') or []:
            space_definition = (self.resources.get('spaces') or {}).get(space_key)
            if isinstance(space_definition, dict):
                depends_sources.append(space_definition.get('dependsOn') or {})
        for depends_on in depends_sources:
            for key in depends_on.get('dashboards') or []:
                if key not in required:
                    required.append(key)
            for key in depends_on.get('optionalDashboards') or []:
                if key not in optional and key not in required:
                    optional.append(key)
            for key in depends_on.get('datasets') or []:
                if key not in datasets:
                    datasets.append(key)
            for arn in depends_on.get('knowledgeBases') or []:
                if arn not in knowledge_bases:
                    knowledge_bases.append(arn)
        return required, optional, datasets, knowledge_bases

    def _deployed_dashboard_arns_by_id(self) -> dict:
        """dashboardId -> Arn strictly from the read-only ListDashboards response."""
        deployed_arns_by_id = {}
        for summary in self.qs.list_dashboards():
            if summary.get('DashboardId') and summary.get('Arn'):
                deployed_arns_by_id[summary['DashboardId']] = summary['Arn']
        return deployed_arns_by_id

    def _deployed_dashboard_arns_or_none(self):
        """Tolerant variant for dependency annotations: None when ListDashboards
        fails, so the caller skips annotations and continues."""
        try:
            return self._deployed_dashboard_arns_by_id()
        except Exception as exc:
            logger.debug(f'ListDashboards failed ({exc}). Dependency annotations will be skipped.')
            return None

    def _present_dashboard_keys(self, required: list, optional: list, deployed_arns_by_id: dict) -> tuple:
        """Resolve dependency catalog keys to deployment presence.

        Returns (present_keys, key_to_dashboard_id). A key missing from the
        dashboards catalog is treated as missing. Shared by the create-agent
        pre-flight and the list-agents dependency annotations so both always
        agree on what is deployed.
        """
        dashboards_catalog = self.resources.get('dashboards') or {}
        key_to_dashboard_id = {}
        for key in list(required) + list(optional):
            dashboard_id = (dashboards_catalog.get(key) or {}).get('dashboardId')
            if not dashboard_id:
                logger.warning(f'Dependency dashboard key {key!r} is not in the catalog. Treating it as missing.')
                continue
            key_to_dashboard_id[key] = dashboard_id
        present_keys = {key for key, dashboard_id in key_to_dashboard_id.items() if dashboard_id in deployed_arns_by_id}
        return present_keys, key_to_dashboard_id

    def _preflight_agent_dependencies(self, definition: dict) -> dict:
        """Read-only dependency pre-flight via ListDashboards — NEVER deploys.

        Resolves each dependency catalog key to its deployed dashboardId and takes that
        dashboard's ARN from the ListDashboards response (no hand-built ARNs).
        Zero present dashboards raise the guidance CidError; otherwise
        the run proceeds with the present set and warns per missing dependency.
        Dataset dependencies are resolved read-only and only
        present dataset ARNs are attached later.
        """
        required, optional, dataset_keys, knowledge_base_arns = self._collect_agent_dependency_keys(definition)
        deployed_arns_by_id = self._deployed_dashboard_arns_by_id()
        present_keys, key_to_dashboard_id = self._present_dashboard_keys(required, optional, deployed_arns_by_id)
        classification = classify_dependencies(required, optional, present_keys)
        summary_lines = self._dependency_summary_lines(required, optional, present_keys)
        if summary_lines:
            cid_print(f"Dependencies of agent <BOLD>{definition.get('name') or definition.get('agentId')}<END>:")
            for summary_line in summary_lines:
                cid_print(summary_line)
        if not classification['present']:
            raise CidError(
                'None of the dashboards this agent needs are deployed in this account and region. '
                'Nothing was created. Deploy the foundational dashboards first — see '
                f'{self.CID_DASHBOARDS_GUIDANCE}. For advanced dashboards, also see '
                f'{self.DATA_COLLECTION_GUIDANCE}.'
            )
        for key in classification['missing_required']:
            cid_print(
                f'<YELLOW>Warning:<END> required dashboard <BOLD>{key}<END> is not deployed. '
                f'To add it, follow {self.CID_DASHBOARDS_GUIDANCE}. '
                'Proceeding with the available dashboards.'
            )
        for key in classification['missing_optional']:
            cid_print(
                f'<YELLOW>Warning:<END> optional dashboard <BOLD>{key}<END> is not deployed. '
                f'To add it, follow {self.DATA_COLLECTION_GUIDANCE}. '
                'Proceeding with the available dashboards.'
            )
        present_dashboard_arns = [deployed_arns_by_id[key_to_dashboard_id[key]] for key in classification['present']]
        # dataset knowledge presence via the existing read-only discovery
        present_dataset_arns = []
        missing_datasets = []
        for key in dataset_keys:
            arn = self._find_deployed_dataset_arn(key)
            if arn:
                present_dataset_arns.append(arn)
            else:
                missing_datasets.append(key)
        if missing_datasets:
            label = 'datasets' if len(missing_datasets) > 1 else 'dataset'
            cid_print(
                f'<YELLOW>Warning:<END> {label} <BOLD>{", ".join(missing_datasets)}<END> not found — '
                'skipped as knowledge. This command does not create datasets.'
            )
        return {
            'classification': classification,
            'dashboard_arns': present_dashboard_arns,
            'dataset_arns': present_dataset_arns,
            'knowledge_base_arns': knowledge_base_arns,
            'deployed_dashboard_arns_by_id': deployed_arns_by_id,
        }

    def _find_deployed_dataset_arn(self, dataset_key: str) -> str:
        """Resolve a dataset catalog key to a deployed dataset ARN via read-only discovery."""
        datasets = self.qs.datasets  # ListDataSets, or dashboard-based discovery on AccessDenied
        catalog_entry = (self.resources.get('datasets') or {}).get(dataset_key) or {}
        catalog_data = catalog_entry.get('data')
        catalog_dataset_id = catalog_data.get('DataSetId') if isinstance(catalog_data, dict) else None
        for candidate_id in (catalog_dataset_id, dataset_key):
            if candidate_id and candidate_id in datasets:
                return datasets[candidate_id].arn
        for dataset in datasets.values():
            if isinstance(dataset, Dataset) and dataset.name == dataset_key:
                return dataset.arn
        return None

    def _cid_managed_dashboard_arns(self, deployed_arns_by_id: dict) -> set:
        """Deployed dashboard ARNs that belong to the CID catalog (stale-removal scope)."""
        managed = set()
        for entry in (self.resources.get('dashboards') or {}).values():
            arn = deployed_arns_by_id.get((entry or {}).get('dashboardId'))
            if arn:
                managed.add(arn)
        return managed

    def _arns_referenced_by_any_agent(self, deployed_arns_by_id: dict) -> set:
        """Deployed dashboard ARNs still referenced by any catalog agent's dependencies."""
        referenced = set()
        dashboards_catalog = self.resources.get('dashboards') or {}
        for entry in (self.resources.get('agents') or {}).values():
            if not isinstance(entry, dict):
                continue
            required, optional, _, _ = self._collect_agent_dependency_keys(entry)
            for key in required + optional:
                arn = deployed_arns_by_id.get((dashboards_catalog.get(key) or {}).get('dashboardId'))
                if arn:
                    referenced.add(arn)
        return referenced

    def _get_resource_tags(self, arn: str) -> dict:
        """Read resource tags, tolerating failure (provenance detection input)."""
        if not arn:
            return {}
        try:
            tags = self.space.client.list_tags_for_resource(ResourceArn=arn).get('Tags') or []
            return {tag['Key']: tag.get('Value') for tag in tags if isinstance(tag, dict) and 'Key' in tag}
        except Exception as exc:
            logger.debug(f'Cannot list tags for {arn} ({exc}).')
            return {}

    def _agent_is_cid_managed(self, agent: dict) -> bool:
        """Dual-mechanism CID_Managed provenance detection for an Agent."""
        return is_cid_managed(self._get_resource_tags(agent.get('Arn')), agent.get('Description'))

    def _resolve_agent_space(self, definition: dict, space_name: str, principal_arn: str) -> tuple:
        """Resolve or create the target Space; returns (space_id, space_arn).

        Bring-your-own path: --space NAME is resolved via SearchSpaces;
        multiple matches raise a CidError listing the ids; no match creates a Space with a
        deterministically derived id. Catalog path: the definition's dependsOn.spaces[0].
        Pre-existing Spaces are reused ADDITIVELY: a non-CID Space is never
        renamed, re-described, tagged, or re-permissioned — only this agent's resources are
        added by the caller.
        """
        if space_name:  # bring-your-own Space
            matching_ids = self.space.find_by_name(space_name)
            if len(matching_ids) > 1:
                raise CidError(
                    f'Multiple Spaces match the name {space_name!r}: {", ".join(sorted(matching_ids))}. '
                    'Please disambiguate by renaming Spaces so the name is unique.'
                )
            if matching_ids:
                return self._reuse_existing_space(matching_ids[0], principal_arn)
            space_id = derive_space_id(space_name)  # deterministic id from the name
            name = space_name
            description = None
        else:
            depends_spaces = (definition.get('dependsOn') or {}).get('spaces') or []
            if not depends_spaces:
                raise CidError(
                    f"Agent {definition.get('agentId')!r} does not declare a Space under 'dependsOn.spaces' "
                    'and no --space was provided. Please declare a Space in the catalog or pass --space.'
                )
            space_key = depends_spaces[0]
            space_definition = self.get_definition('space', name=space_key) or {}
            space_id = derive_space_id(space_key)  # deterministic id from the catalog id
            name = space_definition.get('name') or space_key
            description = space_definition.get('description')
            if self.space.get(space_id) is not None:
                return self._reuse_existing_space(space_id, principal_arn, name=name, description=description)
        cid_print(f'\tSpace: creating <BOLD>{space_id}<END>')
        space_arn = self.space.create_or_update(space_id, name, description=description)
        self.space.write_provenance(space_arn, space_id)   # dual provenance write
        self.space.grant_owner(space_id, principal_arn)    # 16-action owner set
        return space_id, space_arn

    def _reuse_existing_space(self, space_id: str, principal_arn: str, name: str=None, description: str=None) -> tuple:
        """Reuse an existing Space; returns (space_id, space_arn).

        CID-managed Spaces get the idempotent update + provenance + owner grant. Spaces
        lacking the CID_Managed provenance are reused strictly additively: no rename, no
        description rewrite, no tagging of the Space itself, and no permission change.
        """
        space = self.space.get(space_id) or {}
        space_arn = (space.get('spaceArn') or space.get('SpaceArn') or space.get('arn') or space.get('Arn')
                     or f'arn:{self.base.partition}:quicksight:{self.base.region}:{self.base.account_id}:space/{space_id}')
        current_description = str(space.get('description') or space.get('Description') or '')
        if is_cid_managed(self._get_resource_tags(space_arn), current_description):
            cid_print(f'\tSpace: reusing <BOLD>{space_id}<END>')
            self.space.create_or_update(space_id, name or space.get('name') or space.get('Name') or space_id, description=description)
            self.space.write_provenance(space_arn, space_id)
            self.space.grant_owner(space_id, principal_arn)
        else:
            cid_print(
                f'\tSpace: reusing <BOLD>{space_id}<END> additively (not CID-managed: existing '
                'resources, name, description, tags and permissions are left untouched)'
            )
        return space_id, space_arn

    def _attach_knowledge_bases(self, space_id: str, knowledge_base_arns: list):
        """Attach pre-existing knowledge-base ARNs to the Space.

        Never creates a knowledge base or Research asset. Unresolvable
        references are warned about and skipped; attach failures are reported by the
        Space helper and the deployment continues with the dashboards.
        """
        resolvable = []
        for arn in knowledge_base_arns or []:
            if not str(arn).startswith('arn:'):
                cid_print(
                    f'<YELLOW>Warning:<END> the knowledge-base reference {arn!r} cannot be resolved '
                    '(not an ARN). Skipping it and continuing with the dashboards.'
                )
                continue
            resolvable.append(arn)
        if not resolvable:
            return
        failed = self.space.update_resources(space_id, resolvable, resource_type='KNOWLEDGE_BASE')
        if failed:
            cid_print(
                f'<YELLOW>Warning:<END> {len(failed)} knowledge-base resource(s) could not be attached '
                f'to Space {space_id!r}. Continuing with the dashboards.'
            )

    # --- Quick Suite Agent platform: list-agents (live state, no state file) ---

    @command
    def list_agents(self, **kwargs):
        """List catalog agents grouped by category with live deployment status.

        Deployment status is determined per entry via live DescribeAgent by agent id
 — never a local state file and never ListAgents, which omits agents
        in PREVIEW/FAILED states (ListAgents is enumeration-only). Entries in category
        'Deprecated' are hidden. A live-query failure marks that entry with
        an unknown-status indicator and listing continues.
        """
        agents_catalog = self.resources.get('agents') or {}
        if not agents_catalog:
            cid_print('No agents found in the catalog.')
            return {}
        deployed_ids = set()
        unknown_ids = set()
        for key, entry in agents_catalog.items():
            entry = entry if isinstance(entry, dict) else {}
            if str(entry.get('category') or '') == 'Deprecated':
                continue  # hidden entries are never queried
            target_id = entry.get('agentId') or key
            try:
                if self.agent.get(target_id) is not None:  # live DescribeAgent
                    deployed_ids.add(target_id)
            except Exception as exc:
                logger.debug(f'DescribeAgent failed for {target_id!r} ({exc}). Marking unknown status.')
                unknown_ids.add(target_id)  # unknown status; continue with the rest
        # one read-only ListDashboards call covers all dependency annotations
        deployed_dashboard_arns = self._deployed_dashboard_arns_or_none()
        # category grouping, ✓ marking, Deprecated hidden, each entry once
        listing = build_agent_listing(agents_catalog, deployed_ids)
        for category, entries in listing.items():
            cid_print(f'\n<BOLD>{category}<END>')
            for entry in entries:
                display = entry['display']
                if entry['agentId'] in unknown_ids:
                    # unknown-status indicator in place of the check mark
                    display = f" ?[{entry['agentId']}] {entry['name']}"
                provided_by = (agents_catalog.get(entry['key']) or {}).get('providedBy')
                suffix = f'  (provided by {provided_by})' if provided_by else ''
                cid_print(f'{display}{suffix}')
                if deployed_dashboard_arns is not None:
                    definition = agents_catalog.get(entry['key']) or {}
                    for dependency_line in self._agent_dependency_lines(definition, deployed_dashboard_arns):
                        cid_print(dependency_line)
        return listing

    def _agent_dependency_lines(self, definition: dict, deployed_arns_by_id: dict,
                                color: bool=True, indent: str='     ', width: int=None) -> list:
        """Dependency summary lines for a listing entry: declared dashboards
        (agent + its Spaces) with a per-key deployed indicator."""
        required, optional, _, _ = self._collect_agent_dependency_keys(definition)
        present_keys, _ = self._present_dashboard_keys(required, optional, deployed_arns_by_id)
        return self._dependency_summary_lines(required, optional, present_keys,
                                              color=color, indent=indent, width=width)

    @staticmethod
    def _dependency_summary_lines(required: list, optional: list, present_keys,
                                  color: bool=True, indent: str='     ', width: int=None) -> list:
        """Format declared dashboard dependencies with per-key deployed indicators.

        Returns a list of lines wrapped to the terminal width: everything on one
        line when it fits, otherwise one line per group (required / optional) with
        continuation lines aligned under the first key. ``color=False`` yields
        plain marks for surfaces that render literal text (the InquirerPy picker);
        the default carries cid_print color tags.
        """
        if not required and not optional:
            return []
        if width is None:
            width = min(shutil.get_terminal_size((120, 24)).columns, 120)
        present = set(present_keys or ())

        def _marked(key):
            """(rendered text, visible length) — color tags have no visible width."""
            mark = '✓' if key in present else '✗'
            plain = f'{mark} {key}'
            if not color:
                return plain, len(plain)
            tag = 'GREEN' if key in present else 'YELLOW'
            return f'<{tag}>{mark}<END> {key}', len(plain)

        groups = []
        if required:
            groups.append(('required:', [_marked(key) for key in required]))
        if optional:
            groups.append(('optional:', [_marked(key) for key in optional]))

        segments = []   # (rendered segment, visible length) per group
        for label, items in groups:
            text = label + ' ' + '  '.join(item_text for item_text, _ in items)
            visible = len(label) + 1 + sum(item_len for _, item_len in items) + 2 * (len(items) - 1)
            segments.append((text, visible))
        total = len(indent) + sum(visible for _, visible in segments) + 4 * (len(segments) - 1)
        if total <= width:
            return [indent + '    '.join(text for text, _ in segments)]

        lines = []
        for label, items in groups:
            prefix = indent + label + ' '
            continuation = ' ' * len(prefix)
            line_text, line_visible = prefix, len(prefix)
            for position, (item_text, item_len) in enumerate(items):
                if position and line_visible + 2 + item_len > width:
                    lines.append(line_text)
                    line_text, line_visible = continuation + item_text, len(continuation) + item_len
                else:
                    joiner = '  ' if position else ''
                    line_text += joiner + item_text
                    line_visible += (2 if position else 0) + item_len
            lines.append(line_text)
        return lines

    # --- Quick Suite Agent platform: delete-agent (guarded, CID-managed only) ---

    @command
    def delete_agent(self, agent_id: str=None, **kwargs):
        """Delete a CID-managed Quick Agent. NEVER deletes any dashboard.

        Deletion is gated by a yes/no confirmation defaulting to 'no', a
        missing target is treated as success, targets lacking the
        CID_Managed provenance are refused, and the delete retries through
        ConflictException while the agent settles (, in the Agent helper).
        --delete-space removes the agent's Space only when no other CID-managed agent
        references it; --cleanup-space removes only
        CID-managed, unreferenced dashboard resources.
        """
        agents_catalog = self.resources.get('agents') or {}
        agent_key = agent_id or get_parameters().get('agent-id')
        if not agent_key:
            if not agents_catalog:
                raise CidError('No agents found in the catalog. Please provide --agent-id.')
            # category-grouped picker: DEPLOYED badge via ListAgents (enumeration only), hide Deprecated
            listing = build_agent_listing(agents_catalog, self._deployed_agent_ids())
            agent_options = self._agent_picker_options(listing)
            try:
                agent_key = get_parameter(
                    param_name='agent-id',
                    message='Please select an agent to delete',
                    choices=agent_options,
                    fuzzy=False,
                )
            except Exception as exc:
                # non-interactive with no value/default: name the missing input
                raise CidCritical(
                    "Required input 'agent-id' has no supplied value, stored default, or "
                    'fallback in a non-interactive environment. Please provide --agent-id.'
                ) from exc
        # tolerate either the catalog key or the deployed agentId as input
        definition = agents_catalog.get(agent_key)
        if definition is None:
            definition = next(
                (entry for entry in agents_catalog.values()
                 if isinstance(entry, dict) and entry.get('agentId') == agent_key),
                None,
            )
        target_agent_id = (definition or {}).get('agentId') or agent_key

        # missing target = success; existence via DescribeAgent, not ListAgents
        existing = self.agent.get(target_agent_id)
        if existing is None:
            cid_print(f'Agent <BOLD>{target_agent_id}<END> does not exist. Nothing to delete.')
            return target_agent_id
        # refuse targets lacking the CID_Managed provenance — no delete call
        if not self._agent_is_cid_managed(existing):
            raise CidError(
                f'The agent {target_agent_id!r} exists but is not managed by Cloud Intelligence '
                'Dashboards (no CID_Managed provenance). Refusing to delete it. Please remove it '
                'manually if that is really what you want.'
            )
        # confirmation via yes/no parameter defaulting to 'no'; -y/--yes confirms
        if not get_yesno_parameter(
                param_name='confirm-delete',
                message=f'Delete the agent {target_agent_id!r}? (no dashboard will be deleted)',
                default='no'):
            cid_print(f'Deletion of agent <BOLD>{target_agent_id}<END> was not confirmed. Nothing was deleted.')
            return target_agent_id

        agent_space_arns = [str(arn) for arn in existing.get('Spaces') or []]
        # the Agent helper retries through ConflictException while the agent settles
        # and only calls DeleteAgent — no dashboard is ever deleted
        self.agent.delete(target_agent_id)
        cid_print(f'Agent <BOLD>{target_agent_id}<END> deleted. No dashboard was deleted.')

        space_ids = self._agent_space_ids(definition, agent_space_arns)
        retained_space_ids = set(space_ids)
        if get_parameters().get('delete-space'):
            for space_id in sorted(space_ids):
                if self._delete_agent_space(space_id, target_agent_id):
                    retained_space_ids.discard(space_id)
        if get_parameters().get('cleanup-space') and retained_space_ids:
            # scoped removal: CID-managed AND unreferenced by any agent's dependencies
            deployed_arns_by_id = {}
            for summary in self.qs.list_dashboards():
                if summary.get('DashboardId') and summary.get('Arn'):
                    deployed_arns_by_id[summary['DashboardId']] = summary['Arn']
            managed_arns = (
                self._cid_managed_dashboard_arns(deployed_arns_by_id)
                - self._arns_referenced_by_any_agent(deployed_arns_by_id)
            )
            for space_id in sorted(retained_space_ids):
                if self.space.get(space_id) is None:
                    continue
                self.space.update_resources(
                    space_id, [], resource_type='DASHBOARD',
                    remove_stale=True, managed_arns=managed_arns,
                )
        return target_agent_id

    def _agent_space_ids(self, definition: dict, space_arns: list) -> set:
        """Space ids of an agent: catalog `dependsOn.spaces` + the deployed Spaces list."""
        space_ids = set()
        for space_key in ((definition or {}).get('dependsOn') or {}).get('spaces') or []:
            space_ids.add(derive_space_id(space_key))
        for arn in space_arns or []:
            if ':space/' in str(arn):
                space_ids.add(str(arn).split(':space/', 1)[1])
        return space_ids

    def _space_dependent_agents(self, space_id: str, exclude_agent_id: str) -> list:
        """Other deployed, CID-managed catalog agents that reference the Space.

        A reference is either the deployed agent's Spaces list containing the Space or
        the agent's catalog `dependsOn.spaces` deriving to the same space id.
        """
        dependents = []
        for key, entry in (self.resources.get('agents') or {}).items():
            entry = entry if isinstance(entry, dict) else {}
            other_id = entry.get('agentId') or key
            if other_id == exclude_agent_id:
                continue
            try:
                other = self.agent.get(other_id)
            except Exception as exc:
                logger.debug(f'DescribeAgent failed for {other_id!r} ({exc}). Skipping its dependency check.')
                continue
            if other is None or not self._agent_is_cid_managed(other):
                continue
            catalog_refs = {
                derive_space_id(space_key)
                for space_key in (entry.get('dependsOn') or {}).get('spaces') or []
            }
            deployed_refs = {
                str(arn).split(':space/', 1)[1]
                for arn in other.get('Spaces') or [] if ':space/' in str(arn)
            }
            if space_id in catalog_refs or space_id in deployed_refs:
                dependents.append(other_id)
        return dependents

    def _delete_agent_space(self, space_id: str, deleted_agent_id: str) -> bool:
        """Delete the agent's Space unless another CID-managed agent references it.

        Never deletes a Space lacking the CID_Managed provenance (delete-agent manages
        CID-managed resources only). Returns True when the Space is gone (deleted or
        already absent), False when it is retained.
        """
        space = self.space.get(space_id)
        if space is None:
            logger.debug(f'Space {space_id!r} does not exist. Nothing to delete.')
            return True
        dependents = self._space_dependent_agents(space_id, deleted_agent_id)
        if dependents:
            cid_print(
                f'Space <BOLD>{space_id}<END> is retained: it is still referenced by the '
                f"CID-managed agent(s) {', '.join(sorted(dependents))}."
            )
            return False
        space_arn = (space.get('spaceArn') or space.get('SpaceArn') or space.get('arn') or space.get('Arn')
                     or f'arn:{self.base.partition}:quicksight:{self.base.region}:{self.base.account_id}:space/{space_id}')
        description = str(space.get('description') or space.get('Description') or '')
        if not is_cid_managed(self._get_resource_tags(space_arn), description):
            cid_print(
                f'Space <BOLD>{space_id}<END> is retained: it is not managed by Cloud Intelligence '
                'Dashboards (no CID_Managed provenance).'
            )
            return False
        try:
            self.space.client.delete_space(AwsAccountId=self.base.account_id, SpaceId=space_id)
            cid_print(f'Space <BOLD>{space_id}<END> deleted.')
        except self.space.client.exceptions.ResourceNotFoundException:
            logger.debug(f'Space {space_id!r} was already deleted.')
        return True

    @command
    def open(self, dashboard_id, **kwargs):
        """Open QuickSight dashboard in browser"""

        aws_execution_env = os.environ.get('AWS_EXECUTION_ENV', '')
        if  aws_execution_env == 'CloudShell' or aws_execution_env.startswith('AWS_Lambda'):
            print(f"Operation is not supported in {aws_execution_env}")
            return dashboard_id
        if not dashboard_id:
            dashboard_id = self.qs.select_dashboard(force=True)

        dashboard = self.qs.discover_dashboard(dashboard_id)

        logger.info('Getting dashboard status...')
        if not dashboard:
            logger.error(f'{dashboard_id} is not deployed.')
            return None
        if dashboard.version.get('Status') not in ['CREATION_SUCCESSFUL', 'UPDATE_IN_PROGRESS', 'UPDATE_SUCCESSFUL']:
            cid_print(json.dumps(dashboard.version.get('Errors'), indent=4, sort_keys=True, default=str))
            cid_print(f'Dashboard {dashboard_id} is unhealthy, please check errors above.')
        logger.info('healthy, opening...')
        webbrowser.open(self.qs_url.format(dashboard_id=dashboard_id, **self.qs_url_params))

        return dashboard_id

    @command
    def refresh_datasets(self, dashboard_id, **kwargs):
        """Refresh datasets for a dashboard"""
        
        if not dashboard_id:
            dashboard_id = self.qs.select_dashboard(force=True)
            if not dashboard_id:
                print('No dashboard selected')
                return None

        dashboard = self.qs.discover_dashboard(dashboard_id)
        
        if not dashboard:
            logger.error(f'Dashboard {dashboard_id} is not deployed.')
            return None
            
        logger.info(f'Refreshing datasets for dashboard: {dashboard_id}')
        dashboard.refresh_datasets()
        
        return dashboard_id

    @command
    def status(self, dashboard_id, **kwargs):
        """Check QuickSight dashboard status"""
        next_selection = None
        while next_selection != 'exit':
            if not dashboard_id:
                if not self.qs.dashboards:
                    print('No deployed dashboards found')
                    return
                dashboard_id = self.qs.select_dashboard(force=True)
                if not dashboard_id:
                    print('No dashboard selected')
                    return
            dashboard = self.qs.discover_dashboard(dashboard_id)

            if dashboard is not None:
                dashboard.display_status()
                dashboard.display_url(self.qs_url, **self.qs_url_params)
                with IsolatedParameters():
                    next_selection = get_parameter(
                        param_name=f'{dashboard.id}',
                        message="Please make a selection",
                        choices={
                            '[◀] Back': 'back',
                            '[↗] Open': 'open',
                            '[◴] Refresh datasets': 'refresh',
                            '[↺] Update dashboard': 'update',
                            '[✕] Exit': 'exit',
                        }
                    )
                    if next_selection == 'open':
                        self.open(dashboard.id, **kwargs)

                    elif next_selection == 'refresh':
                        dashboard.refresh_datasets()

                    elif next_selection == 'update':
                        if dashboard.latest:
                            if not get_yesno_parameter(
                                    param_name=f'redeploy-{dashboard.id}',
                                    message=f'\nThe selected dashboard {dashboard.id} is already on the latest version.\nDo you want to re-deploy it?',
                                    default='no'):
                                logger.info(f'Not re-deploying {dashboard.id} as it is on latest version.\n')
                                continue
                        recursive = get_parameter(
                            param_name='recursive',
                            message=f'\nRecursive update the Datasets and Views in addition to the Dashboard update?\nATTENTION: This could lead to the loss of dataset customization.\nRecursive update?',
                            choices={
                                '[→] Simple Update (only dashboard)': 'simple',
                                '[⇶] Recursive Update (dashboard and all dependencies)': 'recursive',
                            }
                        ) == 'recursive'
                        logger.info(f'Updating dashboard: {dashboard.id} with Recursive = {recursive}')
                        self._deploy(dashboard_id, recursive=recursive, update=True)
                        logger.info('Rediscover dashboards after update')
                        self.qs.discover_dashboards(refresh_overrides=[dashboard.id])
                self.qs.clear_dashboard_selection()
                dashboard_id = None
            else:
                cid_print('not deployed.')

    @command
    def delete(self, dashboard_id, **kwargs):
        """Delete QuickSight dashboard"""

        # select
        if not dashboard_id:
            if not self.qs.dashboards:
                print('No deployed dashboards')
                return
            dashboard_id = self.qs.select_dashboard(force=True)
            if not dashboard_id:
                return

        # save datasets to destroy later
        if self.qs.dashboards and dashboard_id in self.qs.dashboards:
            datasets = self.qs.discover_dashboard(dashboard_id).datasets # save for later
        else:
            dashboard_definition = self.get_definition("dashboard", id=dashboard_id)
            datasets = {d: None for d in (dashboard_definition or {}).get('dependsOn', {}).get('datasets', [])}

        # delete dash
        try:
            cid_print('Deleting dashboard')
            self.qs.delete_dashboard(dashboard_id=dashboard_id)
            cid_print(f'Dashboard {dashboard_id} deleted')
            self.track('deleted', dashboard_id)
        except self.qs.client.exceptions.ResourceNotFoundException:
            cid_print('not found')
        except Exception as e:
            # Catch exception and dump a reason
            logger.debug(e, exc_info=True)
            cid_print(f'failed with an error message: {e}')
            return dashboard_id

        cid_print('Processing dependencies')
        for dataset_name, dataset_id in datasets.items():
            self.delete_dataset(name=dataset_name, id=dataset_id)

        return dashboard_id

    def delete_dataset(self, name: str, id: str=None):
        if name not in self.resources['datasets']:
            logger.info(f'Dataset {name} is not managed by CID. Skipping.')
            print(f'Dataset {name} is not managed by CID. Skipping.')
            return False
        for dataset in list(self.qs._datasets.values()) if self.qs._datasets else []:
            if dataset.id == id or dataset.name == name:
                # Check if dataset is used in some other dashboard
                for dashboard in (self.qs.dashboards or {}).values():
                    if dataset.id in dashboard.datasets.values():
                        cid_print(f'Dataset {dataset.name} ({dataset.id}) is still used by dashboard "{dashboard.id}". Skipping.')
                        return False
                else: #not used

                    # try to get the database name from the dataset (might need this for later)
                    schema = next(iter(dataset.schemas), None) # FIXME: manage choice if multiple data sources
                    if schema:
                        logger.debug(f'Picking the first of dataset databases: {dataset.schemas}')
                        self.athena.DatabaseName = schema

                    if get_yesno_parameter(
                        param_name=f'confirm-{dataset.name}',
                        message=f'Delete QuickSight Dataset {dataset.name}?',
                        default='no'):
                        print(f'Deleting dataset {dataset.name} ({dataset.id})')
                        self.qs.delete_dataset(dataset.id)
                    else:
                        cid_print(f'Skipping dataset {dataset.name}')
                        return False
                if not dataset.datasources:
                    continue
                datasources = dataset.datasources
                athena_datasource = self.qs.datasources.get(datasources[0])
                if athena_datasource and not get_parameters().get('athena-workgroup'):
                    self.athena.WorkGroup = athena_datasource.AthenaParameters.get('WorkGroup')
                    break
                logger.debug(f'Cannot find QuickSight DataSource {datasources[0]}. So cannot define Athena WorkGroup')
                continue
        else:
            logger.info(f'Dataset not found for deletion: {name} ({id})')
        for view_name in list(set(self.resources['datasets'][name].get('dependsOn', {}).get('views', []))):
            self.delete_view(view_name)
        return True

    def delete_view(self, view_name):
        if view_name not in self.resources['views']:
            logger.info(f'View {view_name} is not managed by CID. Skipping.')
            return False
        logger.info(f'Deleting view "{view_name}"')
        definition = self.get_definition("view", name=view_name, noparams=True)
        if not definition:
            logger.info(f'Definition not found for view: "{view_name}"')
            return False

        for dashboard in (self.qs.dashboards or {}).values():
            if view_name in dashboard.views:
                print(f'View {view_name} is used by dashboard "{dashboard.id}". Skipping')
                return False

        self.athena.discover_views([view_name])
        if view_name not in self.athena._metadata.keys():
            print(f'Table for deletion not found: {view_name}')
        else:
            if definition.get('type', '') == 'Glue_Table':
                print(f'Deleting table: {view_name}')
                self.athena.delete_table(view_name)
            else:
                print(f'Deleting view:  {view_name}')
                self.athena.delete_view(view_name)

        # manage dependencies
        for dependency_view in list(set(definition.get('dependsOn', {}).get('views', []))):
            self.delete_view(dependency_view)

        return True

    @command
    def cleanup(self, **kwargs):
        """Delete unused resources (QuickSight datasets not used in Dashboards)"""

        self.qs.pre_discover()
        self.qs.discover_datasets()
        references = {}
        for dashboard in self.qs.dashboards.values():
            for dataset_id in dashboard.datasets.values():
                if dataset_id not in references:
                    references[dataset_id] = []
                references[dataset_id].append(dashboard.id)
        for dataset in list(self.qs._datasets.values()):
            if dataset.id in references:
                cid_print(f'Dataset {dataset.name} ({dataset.id}) is in use ({", ".join(references[dataset.id])})')
                continue
            if get_yesno_parameter(f'confirm-delete-dataset-{dataset.id}',
                message=f'Delete dataset "{dataset.name}" (not used in dashboards, but can be used in analysis)?',
                default='no',
                ):
                logger.info(f'Deleting dataset {dataset.name} ({dataset.id})')
                self.qs.delete_dataset(dataset.id)
                cid_print(f'Deleted dataset {dataset.name} ({dataset.id})')

    @command
    def share(self, dashboard_id, **kwargs):
        """Share resources (QuickSight datasets, dashboards)"""
        self._share(dashboard_id, **kwargs)


    def _share(self, dashboard_id, **kwargs):
        """Share resources (QuickSight datasets, dashboards)"""

        if not dashboard_id:
            if not self.qs.dashboards:
                print('No deployed dashboards found')
                return
            dashboard_id = self.qs.select_dashboard(force=True)
            if not dashboard_id:
                return
        else:
            # Describe dashboard by the ID given, no discovery
            self.qs.discover_dashboard(dashboard_id)

        dashboard = self.qs.discover_dashboard(dashboard_id)

        if dashboard is None:
            print('not deployed.')
            return

        share_methods = {
            'Shared Folder (except datasource)': 'folder',
            'Specific User only': 'user',
            'Everyone in this account': 'account',
        }
        share_method = get_parameter(
            param_name='share-method',
            message="Please select sharing method",
            choices=share_methods,
        )
        if share_method == 'folder':
            folder = None
            folder_methods = {
                'Select Existing folder': 'existing',
                'Create New folder': 'new'
            }
            folder_method = get_parameter(
                param_name='folder-method',
                message="Please select folder method",
                choices=folder_methods,
            )
            if folder_method == 'existing':
                try:
                    folder = self.qs.select_folder()
                except self.qs.client.exceptions.AccessDeniedException:
                    # If user is not allowed to select folder, prompt for it
                    print('\nYou are not allowed to select folder, please enter folder ID')
                    while not folder:
                        folder_id = get_parameter(
                            param_name='folder-id',
                            message='Please enter the folder Id to use'
                        )
                        folder = self.qs.describe_folder(folder_id)
                    print(f'Selected folder {folder.get("Name")} ({folder.get("FolderId")})')
            elif folder_method == 'new' or not folder:
                # If user is allowed to select folder, but there is no folder exists, prompt to create one
                if folder_method != 'new':
                    print("No folders found, creating one...")
                while not folder:
                    try:
                        folder_name = get_parameter(
                            param_name='folder-name',
                            message='Please enter the folder name to create'
                        )
                        folder_permissions_tpl = Template(
                            (resources.files('cid.builtin.core') / 'data/permissions/folder_permissions.json').read_text()
                        )
                        columns_tpl = {
                            'PrincipalArn': self.qs.get_principal_arn()
                        }
                        folder_permissions = json.loads(folder_permissions_tpl.safe_substitute(columns_tpl))
                        folder = self.qs.create_folder(folder_name, **folder_permissions)
                    except self.qs.client.exceptions.AccessDeniedException:
                        raise CidError('You are not allowed to create folder, unable to proceed')

            self.qs.create_folder_membership(folder.get('FolderId'), dashboard.id, 'DASHBOARD')
            for _id in dashboard.datasets.values():
                self.qs.create_folder_membership(folder.get('FolderId'), _id, 'DATASET')
            print(f'Sharing complete')
        elif share_method in ['account', 'user']:
            if share_method == 'account':
                principal_arn = f"arn:{self.base.partition}:quicksight:{self.qs.identityRegion}:{self.qs.account_id}:namespace/default"
                template_filename = 'data/permissions/dashboard_permissions_namespace.json'
            elif share_method == 'user':
                template_filename = 'data/permissions/dashboard_permissions.json'
                print('Fetching QuickSight users. Duration will scale with the number of users.')
                user = self.qs.select_user()
                while not user:
                    user_name = get_parameter(
                        param_name='quicksight-user',
                        message='Please enter the user name to share with'
                    )
                    user = self.qs.describe_user(user_name)
                    if not user:
                        print(f'QuickSight user {user_name} was not found')
                        unset_parameter('quicksight-user')
                principal_arn = user.get('Arn')

            # Update Dashboard permissions
            columns_tpl = {
                'PrincipalArn': principal_arn
            }
            dashboard_permissions_tpl = Template(
                (resources.files('cid.builtin.core') / template_filename).read_text()
            )
            dashboard_permissions = json.loads(dashboard_permissions_tpl.safe_substitute(columns_tpl))
            dashboard_params = {
                "GrantPermissions": [
                    dashboard_permissions
                ]
            }
            if share_method == 'account':
                dashboard_params.update({
                    "GrantLinkPermissions": [
                        dashboard_permissions
                    ]
                })

            logger.info(f'Sharing dashboard {dashboard.name} ({dashboard.id})')
            try:
                self.qs.update_dashboard_permissions(DashboardId=dashboard.id, **dashboard_params)
                logger.info(f'Shared dashboard {dashboard.name} ({dashboard.id})')
            except self.qs.client.exceptions.AccessDeniedException:
                logger.error('An error occurred (AccessDeniedException) when calling the UpdateDashboardPermissions operation')

            # Update DataSet permissions
            if share_method == 'account':
                logger.info(f'Sharing datasets/datasources with an account is not supported, skipping')
            else:
                data_set_permissions_tpl = Template(
                    (resources.files('cid.builtin.core') / 'data/permissions/data_set_permissions.json').read_text()
                )
                data_set_permissions = json.loads(data_set_permissions_tpl.safe_substitute(columns_tpl))

                _datasources: Dict[str, Datasource] = {}
                for _id in dashboard.datasets.values():
                    logger.info(f'Sharing dataset {_id}')
                    self.qs.update_data_set_permissions(DataSetId=_id, GrantPermissions=[data_set_permissions])
                    logger.info(f'Sharing dataset {_id} complete')
                    _dataset = self.qs._datasets.get(_id)
                    # Extract DataSources from DataSet
                    for v in _dataset.datasources:
                        _datasource = self.qs.describe_data_source(v)
                        if not _datasources.get(_datasource.id):
                            _datasources.update({_datasource.id: _datasource})

                data_source_permissions_tpl = Template(
                    (resources.files('cid.builtin.core') / f'data/permissions/data_source_permissions.json').read_text()
                )
                data_source_permissions = json.loads(data_source_permissions_tpl.safe_substitute(columns_tpl))
                for k, v in _datasources.items():
                    logger.info(f'Sharing data source "{v.name}" ({k})')
                    self.qs.update_data_source_permissions(DataSourceId=k, GrantPermissions=[data_source_permissions])
                    logger.info(f'Sharing data source "{v.name}" ({k}) complete')

            print(f'Sharing complete')

    @command
    def update(self, dashboard_id, recursive=False, force=False, **kwargs):
        """Update Dashboard

        :param dashboard_id: dashboard_id, if None user will be asked to choose
        :param recursive: Update Datasets and Views as well
        :param force: allow selection of already updated dashboards in the manual selection mode
        """
        if not dashboard_id:
            if not self.qs.dashboards:
                print('\nNo deployed dashboards found')
                return
            dashboard_id = self.qs.select_dashboard(force)
            if not dashboard_id:
                if not force:
                    print('\nNo updates available or dashboard(s) is/are broken, use --force to allow selection\n')
                return

        return self._deploy(dashboard_id, recursive=recursive, update=True)


    def check_dashboard_version_compatibility(self, dashboard_id):
        """ Returns True | False | None if could not check """
        try:
            dashboard = self.qs.discover_dashboard(dashboard_id)
        except CidCritical:
            print(f'Dashboard "{dashboard_id}" is not deployed')
            return None

        if dashboard.latest:
            cid_print("You are up to date!")
            cid_print(f"  Version    {dashboard.cid_version}")
        else:
            cid_print(f"An update is available:")
            cid_print(f"  Version    {dashboard.cid_version} ->  {dashboard.latest_available_cid_version}")

        try:
            return dashboard.cid_version.compatible_versions(dashboard.latest_available_cid_version)
        except ValueError as exc:
            logger.info(exc)
        return None

    def update_dashboard(self, dashboard_id, dashboard_definition):

        dashboard = self.qs.discover_dashboard(dashboard_id)
        if not dashboard:
            print(f'Dashboard "{dashboard_id}" is not deployed')
            return

        if isinstance(dashboard.deployed_template, CidQsTemplate):
            print(f'Deployed template: {dashboard.deployed_template.arn}')
        if isinstance(dashboard.source_template, CidQsTemplate):
            print(f"Latest template:   {dashboard.source_template.arn}/version/{dashboard.source_template.version}")
        try:
            cid_print(f'\nUpdating {dashboard_id} from <BOLD>{dashboard.cid_version}<END> to <BOLD>{dashboard.latest_available_cid_version}<END>')
        except:
            cid_print(f'\nUpdating {dashboard_id}')
            logger.debug('Failed to define versions. Still continue.')

        try:
            self.qs.update_dashboard(dashboard, dashboard_definition)
            print('Update completed\n')
            dashboard.display_url(self.qs_url, launch=True, **self.qs_url_params)
            self.track('updated', dashboard_id)
        except Exception as exc:
            # Catch exception and dump a reason
            logger.debug(exc, exc_info=True)
            print(f'failed with an error message: {exc}')

        self.dump_default_parameters()
        return dashboard_id


    def create_datasets(self, _datasets: list, known_datasets: dict={}, recursive: bool=True, update: bool=False) -> dict:
        # Check dependencies
        required_datasets = sorted(_datasets)
        print('\nRequired datasets: \n - {}\n'.format('\n - '.join(list(set(required_datasets)))))

        for dataset_name in required_datasets:
            _ds_id = get_parameters().get(f'{dataset_name.replace("_", "-")}-dataset-id')
            if _ds_id:
                self.qs.describe_dataset(_ds_id)

        existing_datasets = {}
        try:
            existing_datasets = {v['Name']: v['Id'] for v in self.qs.list_data_sets()}
            found_datasets = utils.intersection(required_datasets, existing_datasets.keys())
            for dataset_name in found_datasets:
                if dataset_name not in known_datasets:
                    known_datasets[dataset_name] = found_datasets[dataset_name]
        except:
            found_datasets = utils.intersection(required_datasets, known_datasets.keys())
        missing_datasets = utils.difference(required_datasets, found_datasets)


        logger.debug('known_datasets = %s', known_datasets)
        logger.debug('found_datasets = %s', found_datasets)
        logger.debug('missing_datasets = %s', missing_datasets)

        update = update or get_parameters().get('update')
        # Update existing datasets
        if update:
            logger.debug('updating datasets')
            for dataset_name in found_datasets[:]:
                if dataset_name in known_datasets.keys():
                    _found_dsc = self.qs.get_datasets(id=known_datasets.get(dataset_name))
                    if len(_found_dsc) != 1:
                        logger.warning(f'Found more than one dataset in known datasets with name {dataset_name} {len(_found_dsc)}. Taking the first one.')
                    dataset_id = _found_dsc[0].id
                else:
                    datasets = self.qs.get_datasets(name=dataset_name)
                    if not datasets:
                        continue
                    elif len(datasets) == 1:
                        dataset_id = datasets[0].id
                    else:
                        dataset_id = get_parameter(
                            param_name=f'{dataset_name}-dataset-id',
                            message=f'Multiple "{dataset_name}" datasets detected, please select one',
                            choices=[v.id for v in datasets],
                            default=datasets[0].id
                        )
                    known_datasets[dataset_name] = dataset_id
                cid_print(f'Updating dataset: "{dataset_name}"')
                try:
                    dataset_definition = self.get_definition("dataset", name=dataset_name)
                    if not dataset_definition:
                        cid_print(f'Dataset definition not found, skipping {dataset_name}')
                        continue
                except Exception as e:
                    logger.critical('dashboard definition is broken, unable to proceed.')
                    logger.critical(f'dataset definition not found: {dataset_name}')
                    logger.critical(e, exc_info=True)
                    raise
                try:
                    result = self.create_or_update_dataset(dataset_definition, dataset_id, recursive=recursive, update=update)
                    if result and result != 'skipped':
                        print(f'Updated dataset: "{dataset_name}"')
                    elif not result:
                        print(f'Dataset "{dataset_name}" update failed, collect debug log for more info')
                except self.qs.client.exceptions.AccessDeniedException as exc:
                    print(f'Unable to update, missing permissions: {exc}')
                except Exception as e:
                    logger.debug(e, exc_info=True)
                    raise


        # Look by DataSetId from dataset_template file
        if missing_datasets:
            # Look for previously saved deployment info
            cid_print('Looking by DataSetId defined in template...')
            for dataset_name in missing_datasets[:]:
                try:
                    dataset_definition = self.get_definition(type='dataset', name=dataset_name, noparams=True)
                    raw_template = self.get_data_from_definition(dataset_definition)
                    if raw_template:
                        ds = self.qs.describe_dataset(raw_template.get('DataSetId'))
                        if isinstance(ds, Dataset) and ds.name == dataset_name:
                            missing_datasets.remove(dataset_name)
                            known_datasets[dataset_name] = ds.id
                            print(f"\n\tFound {dataset_name} as {raw_template.get('DataSetId')}")

                except FileNotFoundError:
                    logger.info(f'Definitions File for Dataset "{dataset_name}" not found')
                    pass
                except self.qs.client.exceptions.ResourceNotFoundException:
                    logger.info(f'Dataset "{dataset_name}" not found')
                    pass
                except self.qs.client.exceptions.AccessDeniedException:
                    logger.info(f'Access denied trying to find dataset "{dataset_name}"')
                    pass
                except Exception as e:
                    logger.debug(e, exc_info=True)
            print('complete')

        # If there still datasets missing try automatic creation
        if missing_datasets:
            missing_str = ', '.join(missing_datasets)
            cid_print(f'\nThere are still {len(missing_datasets)} datasets missing: {missing_str}')

            # get rls status of existing datasets
            found_dataset_objects = [self.qs.describe_dataset(ds_id) for ds_id in found_datasets]
            rls_dataset_arns = [ds.rls_arn for ds in found_dataset_objects if ds and ds.rls_arn]
            if rls_dataset_arns:
                rls_dataset_arn = max(set(rls_dataset_arns), key=rls_dataset_arns.count) #get the most frequent
                if not get_parameters().get('rls-dataset-id') and not get_parameters().get('rls'):
                    rls_dataset_id = rls_dataset_arn.split('/')[-1]
                    cid_print('Existing datasets are linked to RLS {rls_dataset_id}. We will reuse it for {missing_str} datasets.')
                    set_parameters({'rls-dataset-id': rls_dataset_id})

            for dataset_name in missing_datasets[:]:
                dataset_id = known_datasets.get(dataset_name)
                print(f'Creating dataset: {dataset_name}')
                try:
                    dataset_definition = self.get_definition("dataset", name=dataset_name)
                    if not dataset_definition:
                        raise Exception(f'Failed to find dataset {dataset_name}. Check if Datasets section in your resources file has that.')
                except Exception as e:
                    logger.critical('dashboard definition is broken, unable to proceed.')
                    logger.critical(f'dataset definition not found: {dataset_name}')
                    logger.critical(e, exc_info=True)
                    raise
                try:
                    if self.create_or_update_dataset(dataset_definition, dataset_id, recursive=recursive, update=update):
                        missing_datasets.remove(dataset_name)
                        known_datasets[dataset_name] = dataset_id
                        print(f'Dataset "{dataset_name}" created')
                    else:
                        print(f'Dataset "{dataset_name}" creation failed, collect debug log for more info')
                except self.qs.client.exceptions.AccessDeniedException as e:
                    print(f'Unable to create dataset  "{dataset_name}", missing permissions')
                    logger.info(f'Unable to create dataset  "{dataset_name}", missing permissions')
                    logger.debug(e, exc_info=True)
                except Exception as e:
                    logger.debug(e, exc_info=True)
                    raise

        # Last chance to enter DataSetIds manually by user
        if missing_datasets:
            missing_str = '\n - '.join(missing_datasets)
            print(f'\nThere are still {len(missing_datasets)} datasets missing: \n - {missing_str}')
            print(f"\nCan't move forward without full list, please manually create datasets and provide DataSetIds")
            # Loop over the list unless we get it empty
            while missing_datasets:
                # Make a copy and then get an item from the list
                dataset_name = missing_datasets.copy().pop()
                _id = get_parameter(
                    param_name=f'{dataset_name}-dataset-id',
                    message=f'DataSetId/Arn for {dataset_name}'
                )
                id = _id.split('/')[-1]
                try:
                    _dataset = self.qs.describe_dataset(id)
                    if _dataset.name != dataset_name:
                        print(f"\tFound dataset with a different name: {_dataset.name}, please provide another one")
                        unset_parameter(f'{dataset_name}-dataset-id')
                        continue
                    self.qs._datasets.update({dataset_name: _dataset})
                    missing_datasets.remove(dataset_name)
                    known_datasets[dataset_name] = _dataset.id
                    print(f'\tFound valid "{_dataset.name}" dataset, using')
                    logger.info(f'\tFound valid "{_dataset.name}" ({_dataset.id}) dataset, using')
                except Exception as e:
                    logger.debug(e, exc_info=True)
                    print(f"\tProvided DataSetId '{id}' can't be found\n")
                    unset_parameter(f'{dataset_name}-dataset-id')
                    continue
        return known_datasets


    def get_data_from_definition(self, definition):
        """ Returns an json object for json resource file and a text for all other definitions
        """
        data = None
        if definition.get('Data') or definition.get('data'):
            data = definition.get('Data') or definition.get('data')
        elif definition.get('url') or definition.get('File') or definition.get('file'):
            source = definition.get('url') or definition.get('File') or definition.get('file')
            assert definition.get('source'), str(definition)
            text =  self.load_text_file(source, definition.get('source'))
            if source.endswith('.json') or source.endswith('.jsn'):
                data = json.loads(text)
            elif source.endswith('.yaml') or source.endswith('.yml'):
                data = yaml.safe_load(text)
            else:
                data = text
        if data is None:
            raise CidCritical(f"Error: definition is broken. Cannot find data for {repr(definition)}. Check resources file.")
        return data


    def create_datasource(self, datasource_id) -> str:
        """Create datasource with given id
        uses parameters: 'quicksight-datasource-role-arn', 'quicksight-datasource-role'
        """
        role_arn = get_parameters().get('quicksight-datasource-role-arn')
        if not role_arn:
            role_name = get_parameters().get('quicksight-datasource-role')
            if role_name:
                role_arn = self.iam.get_role_arn(role_name)

        if not role_arn:
            quicksight_trusted_roles = list(self.iam.iterate_role_names(search="Roles[?AssumeRolePolicyDocument.Statement[?Principal.Service=='quicksight.amazonaws.com']].RoleName"))
            quicksight_trusted_roles = [role for role in quicksight_trusted_roles if role not in ('aws-quicksight-secretsmanager-role-v0')] # filter out irrelevant roles
            # TODO: filter only roles with Athena and S3 policies
            cid_role_name = 'CidCmdQuickSightDataSourceRole'
            choices = quicksight_trusted_roles
            default = None
            if cid_role_name not in choices:
                choices.append(cid_role_name + ' <ADD NEW ROLE>' )
                default = cid_role_name + ' <ADD NEW ROLE>'
            else:
                default = cid_role_name
            choice = get_parameter(
                'quicksight-datasource-role',
                message='Please choose a QuickSight role. It must have access to Athena',
                choices=['<USE DEFAULT QuickSight ROLE (You will need to login to QuickSight (https://quicksight.aws.amazon.com/sn/admin#aws) and configure S3 and Athena access there)>'] + choices,
                default=default,
            )
            if "<ADD NEW ROLE>" in choice or choice == cid_role_name: # Create or update role
                # TODO: get buckets from dashboard parameters
                buckets = [
                    f'cid-{self.base.account_id}-shared',
                    f'cid-{self.base.account_id}-data-exports',
                    f'cid-data-{self.base.account_id}',
                    f'costoptimizationdata{self.base.account_id}',
                ]
                additional_buckets = get_parameters().get('allow-buckets')
                if additional_buckets:
                    buckets += [bucket.strip().replace('{account_id}', self.base.account_id) for bucket in additional_buckets.split(',')]

                databases = set([
                    "optimization_data",
                    "cid_data_collection",
                    "cid_data_export", # prefix for data-exports hardcoded here
                    self.athena.DatabaseName,
                ])
                role_name = self.iam.ensure_data_source_role_exists(
                    role_name=cid_role_name,
                    databases=databases,
                    workgroup=self.athena.WorkGroup,
                    buckets=buckets,
                    output_location_bucket = self.athena.workgroup_output_location().split('/')[2],
                )
                cid_print(f'Role {role_name} was updated. https://console.aws.amazon.com/iam/home?#/roles/details/{role_name}')
                role_arn = self.iam.get_role_arn(role_name)
            elif 'USE DEFAULT QuickSight ROLE' in choice or choice == 'default':
                role_arn = None
            else:
                role_arn = self.iam.get_role_arn(choice)

        athena_datasource = self.qs.create_data_source(
            athena_workgroup=self.athena.WorkGroup,
            datasource_id=datasource_id,
            role_arn=role_arn,
        )
        print('athena_datasource', athena_datasource)
        return athena_datasource



    def _get_dataset_account_map_columns(self, compiled_dataset: dict) -> list:
        """ Return account_map column names if the dataset joins the account_map table.

        Columns are read from Athena so that customer-added attributes
        (business unit, cost center etc.) are included. Returns [] if
        the dataset does not use account_map or the table cannot be read.
        """
        for table in (compiled_dataset.get('PhysicalTableMap') or {}).values():
            relational_table = table.get('RelationalTable', {})
            if relational_table.get('Name') != 'account_map':
                continue
            try:
                # no_cache: same as Dataset.patch, the view can be updated within this run
                metadata = self.athena.get_table_metadata('account_map', database_name=relational_table.get('Schema'), no_cache=True)
                return [col.get('Name') for col in metadata.get('Columns', [])]
            except Exception as exc: #pylint: disable=broad-exception-caught
                logger.debug(f'Cannot read account_map columns: {exc}')
                return [col.get('Name') for col in relational_table.get('InputColumns', [])]
        return []

    def create_or_update_dataset(self, dataset_definition: dict, dataset_id: str=None,recursive: bool=True, update: bool=False) -> bool:
        # Read dataset definition from template
        data = self.get_data_from_definition(dataset_definition)
        template = Template(json.dumps(data))
        cur1_required = dataset_definition.get('dependsOn', dict()).get('cur') or dataset_definition.get('dependsOn', dict()).get('cur1')
        cur2_required = dataset_definition.get('dependsOn', dict()).get('cur2')
        athena_datasource = None

        # Manage datasource
        # We must do it here. In case if datasource is not defined by user, we can take it from dataset

        datasource_id = get_parameters().get('quicksight-datasource-id')
        if datasource_id:
            # We have explicit choice of datasource
            try:
                athena_datasource = self.qs.describe_data_source(datasource_id)
            except self.qs.client.exceptions.ResourceNotFoundException:
                logger.info(f'DataSource {datasource_id} not found. Creating.')
                athena_datasource = self.create_datasource(datasource_id)
            except self.qs.client.exceptions.AccessDeniedException:
                logger.warning(f'AccessDenied reading QuickSight DataSource {datasource_id}. Trying to continue.')
                athena_datasource = Datasource(raw={
                    'AthenaParameters':{},
                    "Id": datasource_id,
                    "Arn": f"arn:{self.base.partition}:quicksight:{self.base.session.region_name}:{self.base.account_id}:datasource/{datasource_id}",
                })
            except Exception as exc:
                raise CidCritical(
                    f'quicksight-datasource-id={datasource_id} not found or not in a valid state.'
                ) from exc
        else:
            # We have no explicit DataSource in parameters
            # QuickSight DataSources are not obvious for customer so we will try to do our best guess
            # - if there is just one? -> silently take that one
            # - if DataSource is references in existing DataSet? -> silently take that one
            # - if athena WorkGroup defined -> Try to find a DataSource with this WorkGroup
            # - and if still nothing -> ask an explicit choice from the user
            pre_compiled_dataset = json.loads(template.safe_substitute())
            dataset_name = pre_compiled_dataset.get('Name')

            # let's find the schema/database and workgroup name
            schemas = []
            datasources = []
            if dataset_id:
                schemas = self.qs.get_datasets(id=dataset_id)[0].schemas
                datasources = self.qs.get_datasets(id=dataset_id)[0].datasources
            else: # try to find dataset and get athena database
                found_datasets = self.qs.get_datasets(name=dataset_name)
                logger.debug(f'Related to dataset {dataset_name}: {[ds.id for ds in found_datasets]}')
                if found_datasets:
                    schemas = list(set(sum([d.schemas for d in found_datasets], [])))
                    datasources = list(set(sum([d.datasources for d in found_datasets], [])))
                    logger.debug(f'Found following schemas={schemas}, related to dataset with name {dataset_name}')
            logger.info(f'Found {len(datasources)} Athena DataSources related to the DataSet {dataset_name}')

            if not get_parameters().get('athena-database') and len(schemas) == 1 and schemas[0]:
                logger.debug(f'Picking the database={schemas[0]}')
                self.athena.DatabaseName = schemas[0]
            # else user will be suggested to choose database anyway

            if len(datasources) == 1 and datasources[0] in self.qs.athena_datasources:
                athena_datasource = self.qs.get_datasources(id=datasources[0])[0]
                logger.info(f'Silently selecting the only available DataSources from other datasets: {datasources[0]}.')
            else:
                # Ask user to choose the datasource
                # Narrow the choice to only datasources with the given workgroup
                datasources_with_workgroup = self.qs.get_datasources(athena_workgroup_name=self.athena.WorkGroup)
                logger.info(f'Found {len(datasources_with_workgroup)} Athena DataSources with WorkGroup={self.athena.WorkGroup}.')
                datasource_choices = {
                    f"{datasource.name} {datasource.id} (workgroup={datasource.AthenaParameters.get('WorkGroup')})": datasource.id
                    for datasource in datasources_with_workgroup
                }
                if 'CID-CMD-Athena' not in list(datasource_choices.values()):
                    datasource_choices['CID-CMD-Athena <CREATE NEW DATASOURCE>'] = 'Create New DataSource'
                #TODO: add possibility to update datasource and role
                datasource_id = get_parameter(
                    param_name='quicksight-datasource-id',
                    message=f"Please choose DataSource (Select the first one if not sure)",
                    choices=datasource_choices,
                )
                if not datasource_id or datasource_id == 'Create New DataSource':
                    datasource_id = 'CID-CMD-Athena'
                    logger.info(f'Creating DataSource {datasource_id}')
                    athena_datasource = self.create_datasource(datasource_id)
                    set_parameters(parameters={'quicksight-datasource-id': datasource_id}) # for next usage
                else:
                    athena_datasource = self.qs.get_datasources(id=datasource_id)[0]
                logger.info(f'Using  DataSource = {athena_datasource.id if athena_datasource else "N/A"}')
        if not get_parameters().get('athena-workgroup'):
            # set default workgroup from datasource if not provided via parameters
            if isinstance(athena_datasource, Datasource) and athena_datasource.AthenaParameters.get('WorkGroup', None):
                self.athena.WorkGroup = athena_datasource.AthenaParameters.get('WorkGroup')
            else:
                logger.debug('Athena_datasource is not defined. Will only create views')

        # attach roles
        if isinstance(athena_datasource, Datasource) and athena_datasource.role_name:
            data_providers = dataset_definition.get('dependsOn', {}).get('dataProviders', [])
            policies_arns = [self.cfn.get_read_access_policy_for_module(provider) for provider in data_providers]
            policies_arns = [policies_arn for policies_arn in policies_arns if policies_arn] # filter out nones
            if policies_arns:
                self.iam.ensure_managed_policies_attached(role_name=athena_datasource.role_name, policies_arns=','.join(policies_arns))

        # Check for required views
        _views = dataset_definition.get('dependsOn', {}).get('views', [])
        required_views = _views

        self.athena.discover_views(required_views)
        found_views = utils.intersection(required_views, self.athena._metadata.keys())
        missing_views = utils.difference(required_views, found_views)

        if recursive:
            print(f"Detected views: {', '.join(found_views)}")
            for view_name in found_views:
                #if cur_required and view_name == self.cur.table_name:
                #    logger.debug(f'Dependency view {view_name} is a CUR. Skip.')
                #    continue
                if view_name == 'account_map':
                    logger.debug(f'Dependency view is {view_name}. Skip.')
                    continue
                self.create_or_update_view(view_name, recursive=recursive, update=update)

        # create missing views
        if len(missing_views):
            print(f"Missing views: {', '.join(missing_views)}")
            for view_name in missing_views:
                self.create_or_update_view(view_name, recursive=recursive, update=update)

        if not isinstance(athena_datasource, Datasource):
            print('athena_datasource is not defined')
            return False
        # Proceed only if all the parameters are set
        columns_tpl = {
            'athena_datasource_arn': athena_datasource.arn,
            'athena_database_name': self.athena.DatabaseName,
            'cur_database':    self.cur1.database   if cur1_required else None, # for backward compatibly
            'cur_table_name':  self.cur1.table_name if cur1_required else None, # for backward compatibly
            'cur1_database':   self.cur1.database   if cur1_required else None,
            'cur1_table_name': self.cur1.table_name if cur1_required else None,
            'cur2_database':   self.cur2.database   if cur2_required else None,
            'cur2_table_name': self.cur2.table_name if cur2_required else None,
        }

        logger.debug(f'dataset_id={dataset_id}')
        logger.debug(f'columns_tpl={columns_tpl}')

        columns_tpl = self.get_template_parameters(
            dataset_definition.get('parameters', dict()),
            f"dataset-{dataset_id or data.get('DataSetId')}-",
            columns_tpl,
        )
        logger.debug(columns_tpl)
        compiled_dataset_text = template.safe_substitute(columns_tpl)
        try:
            compiled_dataset = json.loads(compiled_dataset_text)
        except json.JSONDecodeError as exc:
            logger.error('The json of dataset is not correct. Please check parameters of the dashboard.')
            logger.debug(compiled_dataset_text)
            raise
        if dataset_id:
            compiled_dataset.update({'DataSetId': dataset_id})

        # patch dataset for tags
        cur_tags_json_required = False
        for dep_view_name in dataset_definition.get('dependsOn', {}).get('views', []):
            try:
                tags_type = self.resources['views'][dep_view_name]['dependsOn']['tags']
            except (KeyError, TypeError, AttributeError):
                tags_type = None
            try:
                param_res_tag =  self.resources['views'][dep_view_name]['parameters']['resource_tags']
            except (KeyError, TypeError, AttributeError):
                param_res_tag = None
            if tags_type == 'json' or param_res_tag:
                cur_tags_json_required = True
                break
        custom_fields = {}
        resource_tags = get_parameters().get('resource-tags', [])
        if isinstance(resource_tags, str):
            resource_tags = [t for t in resource_tags.split(',') if t]
        logger.debug(f'dataset {compiled_dataset.get("Name")} resource_tags = {resource_tags}')
        if cur_tags_json_required and resource_tags:
            custom_fields = {
                name: f"parseJson(tags_json, '$.{name.strip()}')" # This syntax does not work:  $[\"{name}\"]
                for name in resource_tags
            }

        # patch dataset for taxonomy merges: combine tag fields and account_map columns
        # into single calculated fields (no Athena view changes, QuickSight level only)
        if custom_fields: # only if the user selected tags/cost categories; ids are join keys, not taxonomy
            account_map_columns = [
                col for col in self._get_dataset_account_map_columns(compiled_dataset)
                if col not in ('account_id', 'parent_account_id')
            ]
            taxonomy_merger = TaxonomyMerger(
                tag_fields=list(custom_fields.keys()),
                account_map_columns=account_map_columns,
            )
            merges = taxonomy_merger.resolve_merges()
            if merges:
                custom_fields.update(taxonomy_merger.build_custom_fields(merges))
        logger.debug(f'custom_fields = {custom_fields}')
        compiled_dataset = Dataset.patch(dataset=compiled_dataset, custom_fields=custom_fields, athena=self.athena)
        logger.trace(f"compiled_dataset = {json.dumps(compiled_dataset)}")
        found_dataset = self.qs.describe_dataset(compiled_dataset.get('DataSetId'), timeout=0)

        rls_dataset_id = get_parameters().get('rls-dataset-id')
        rls_dataset_status = get_parameters().get('rls')
        if rls_dataset_status == 'CLEAR':
            logger.debug('deleting rls')
            if "RowLevelPermissionDataSet" in compiled_dataset:
                del compiled_dataset["RowLevelPermissionDataSet"]
            if isinstance(found_dataset, Dataset) and "RowLevelPermissionDataSet" in found_dataset.raw:
                del found_dataset.raw["RowLevelPermissionDataSet"]

        elif rls_dataset_id or rls_dataset_status:
            if not rls_dataset_id:
                for ds in self.qs.list_data_sets():
                    print(ds)
                choices = {
                    f"{ds.get('Name')}({ds.get('DataSetId')})":ds.get('DataSetId')
                    for ds in self.qs.list_data_sets()
                    if ds.get('UseAs') == 'RLS_RULES'
                }
                if not choices:
                    raise CidCritical(f"Cannot find RLS DataSets")
                rls_dataset_id = get_parameter('rls-dataset-id', message='Select RLS dataset', choices=choices)
            rls_dataset = self.qs.describe_dataset(id=rls_dataset_id)
            if not rls_dataset:
                raise CidCritical(f"RLS DataSet {rls_dataset_id} not found")
            if not rls_dataset.is_rls:
                raise CidCritical(f"DataSet {rls_dataset_id} is not RLS")
            rls_permissions = {
                "Arn": rls_dataset.arn,
                "Status": (rls_dataset_status
                    or (found_dataset and found_dataset.rls_status)
                    or "ENABLED"
                ),
                "PermissionPolicy": "GRANT_ACCESS",
            }
            cols = [c['Name'] for c in rls_dataset.columns]
            if 'UserName' in cols or 'GroupName' in cols:
                rls_permissions['FormatVersion'] = "VERSION_1"
            elif 'UserARN' in cols or 'GroupARN' in cols:
                rls_permissions['FormatVersion'] = "VERSION_2"
            else:
                raise CidCritical(f"DataSet {rls_dataset_id} must have 'UserName'/'GroupName' or 'UserARN'/'GroupARN' columns.")
            compiled_dataset["RowLevelPermissionDataSet"] = rls_permissions

        if isinstance(found_dataset, Dataset):
            update_dataset = False
            if update:
                update_dataset = True
            elif found_dataset.name != compiled_dataset.get('Name'):
                print(f"Dataset found with name {found_dataset.name}, but {compiled_dataset.get('Name')} expected. Updating.")
                update_dataset = True
            if update_dataset and get_parameters().get('on-drift', 'show').lower() != 'override' and isatty() and not cur1_required and not cur2_required:
                while True:
                    diff = self.qs.dataset_diff(found_dataset.raw, compiled_dataset)
                    if diff and diff['diff']:
                        cid_print(f'<BOLD>Found a difference between existing dataset <YELLOW>{found_dataset.name}<END> <BOLD>and the one we want to deploy. <END>')
                        cid_print(diff['printable'])
                        choice = get_parameter(
                            param_name='dataset-' + found_dataset.name.lower().replace(' ', '-') + '-override',
                            message=f'The existing dataset is different. Override?',
                            choices=['retry diff', 'proceed and override', 'keep existing', 'exit'],
                            yes_choice='proceed and override'
                        )
                        if choice == 'retry diff':
                            unset_parameter('dataset-' + found_dataset.name.lower().replace(' ', '-') + '-override')
                            continue
                        elif choice == 'proceed and override':
                            update_dataset = True
                            break
                        elif choice == 'keep existing':
                            update_dataset = False
                            break
                        else:
                            raise CidCritical(f'User choice is not to update {found_dataset.name}.')
                    elif not diff:
                        if get_parameter(
                            param_name=found_dataset.name.lower().replace(' ', '-') + '-override',
                            message=f'Cannot get sql diff for {found_dataset.name}. Continue?',
                            choices=['override', 'exit'],
                            ) != 'override':
                            raise CidCritical(f'User choice is not to update {found_dataset.name}.')
                        update_dataset = True
                    break

            identical = Dataset.datasets_are_identical(found_dataset, compiled_dataset) # check if dataset needs an update

            if update_dataset and not identical:
                merged_dataset = Dataset.merge_datasets(compiled_dataset, found_dataset)
                # Cannot update a legacy dataset to new experience in-place — must delete and recreate
                if Dataset._is_new_experience(compiled_dataset) and not Dataset._is_new_experience(found_dataset.raw):
                    cid_print(f'<BOLD><YELLOW>Important!<END> <BOLD>Dataset <YELLOW>{found_dataset.name}<END> <BOLD>will be updated to new QuickSight Data Preparation Experience as a part of this update.<END>')
                    proceed_with_migration = self._confirm_dataset_experience_migration(found_dataset)
                    if proceed_with_migration:
                        logger.info(f'Dataset {found_dataset.name} is legacy but template uses new experience. Recreating.')
                        existing_permissions = self.qs.describe_data_set_permissions(found_dataset.id)
                        self.qs.delete_dataset(found_dataset.id)
                        # Wait for deletion to complete before creating — API is async
                        for attempt in range(30):
                            try:
                                self.qs.create_dataset(merged_dataset)
                                break
                            except self.qs.client.exceptions.ConflictException:
                                logger.debug(f'Dataset deletion still in progress, waiting... (attempt {attempt + 1}/30)')
                                time.sleep(2)
                        else:
                            raise CidError(f'Timed out waiting for dataset {found_dataset.id} deletion to complete.')

                        if existing_permissions:
                            cid_print(f'Reapplying {len(existing_permissions)} permission entries to dataset {found_dataset.name}')
                            try:
                                self.qs.update_data_set_permissions(
                                    DataSetId=found_dataset.id,
                                    GrantPermissions=existing_permissions
                                )
                            except Exception as e:
                                logger.warning(f'Failed to reapply permissions for dataset {found_dataset.name} ({found_dataset.id}): {e}')
                                cid_print(f"<BOLD><YELLOW>Note:<END> Couldn't transfer existing dataset permissions to the new dataset experience for <BOLD>{found_dataset.name}<END>. If your datasets were shared with someone else, re-share them manually after update.")
                                cid_print(f'Previous permissions were:\n{json.dumps(existing_permissions, indent=2)}')
                        else:
                            cid_print(f"<BOLD><YELLOW>Note:<END> Couldn't read existing dataset permissions for <BOLD>{found_dataset.name}<END>. If your datasets were shared with someone else, re-share them manually after update.")
                    else:
                        logger.info(f'User chose to skip new experience migration for dataset {found_dataset.name}. Skipping dataset update.')
                        cid_print(f'Skipping dataset <BOLD>{found_dataset.name}<END> update.')
                        return 'skipped'  # Dataset exists and is usable, just not migrated
                else:
                    self.qs.update_dataset(merged_dataset)
                if compiled_dataset.get("ImportMode") == "SPICE":
                    dataset_id = compiled_dataset.get('DataSetId')
                    schedules_definitions = []
                    for schedule_name in dataset_definition.get('schedules', []):
                        schedules_definitions.append(self.get_definition("schedule", name=schedule_name))
                        self.qs.ensure_dataset_refresh_schedule(dataset_id, schedules_definitions)
            else:
                print(f'No update requested for dataset {compiled_dataset.get("DataSetId")} {compiled_dataset.get("Name")}={found_dataset.name} ')
        else:
            dataset_id = self.qs.create_dataset(compiled_dataset)
            if dataset_id and compiled_dataset.get("ImportMode") == "SPICE":
                schedules_definitions = []
                for schedule_name in dataset_definition.get('schedules', []):
                    schedules_definitions.append(self.get_definition("schedule", name=schedule_name))
                    self.qs.ensure_dataset_refresh_schedule(dataset_id, schedules_definitions)
        return True


    def create_or_update_view(self, view_name: str, recursive: bool=True, update: bool=False) -> None:
        # Avoid checking a views multiple times in one cid session
        update = update or get_parameters().get('update')
        logger.trace(f'create_or_update_view({view_name}, recursive={recursive}, update={update})')
        if view_name in self._visited_views:
            logger.trace(f'{view_name} is in _visited_views.skipping')
            return
        self._visited_views.append(view_name)
        logger.info(f'Processing view: {view_name}')

        # For account mappings create a view using a special helper
        if view_name in ['account_map', 'aws_accounts']:
            if view_name in self.athena._metadata.keys() and (not update and not recursive):
                print(f'Account map {view_name} exists. Skipping.')
            else:
                self.create_or_update_account_map(view_name)
            return

        # Create a view
        logger.info(f'Getting view definition {view_name}')
        view_definition = self.get_definition("view", name=view_name, noparams=True)
        if not view_definition and view_name in self.athena._metadata.keys():
            logger.info(f"Definition is unavailable but view exists: {view_name}, skipping")
            return
        if not view_definition:
            logger.info(f"Definition is unavailable {view_name}")
            return
        logger.debug(f'View definition: {view_definition}')

        # Dynamic FOCUS consolidation view: discover tables and generate SQL dynamically
        if view_definition.get('type') == 'dynamic_focus_consolidation':
            from cid.helpers.focus_consolidation import FocusConsolidationView
            columns = view_definition.get('columns')
            focus_view = FocusConsolidationView(athena=self.athena, columns=columns)
            if focus_view.create_or_update_view():
                assert self.athena.wait_for_view(view_name), f"Failed to create/update {view_name}"
                logger.info(f'Dynamic view "{view_name}" created/updated')
            else:
                logger.warning(f'Dynamic view "{view_name}" was not created (no FOCUS tables found)')
            return

        dependencies = view_definition.get('dependsOn', {})

        # Process CUR columns
        if dependencies.get('cur') or dependencies.get('cur1'):
            self.cur1.ensure_columns(dependencies.get('cur') or dependencies.get('cur1'))
        if dependencies.get('cur2'):
            self.cur2.ensure_columns(dependencies.get('cur2'))

        if recursive:
            dependency_views = dependencies.get('views', [])
            if 'cur' in dependency_views:
                dependency_views.remove('cur')
            if 'cur2' in dependency_views:
                dependency_views.remove('cur2')
            # Discover dependency views (may not be discovered earlier)
            self.athena.discover_views(dependency_views)
            logger.info(f"Dependency views: {', '.join(dependency_views)}" if dependency_views else 'No dependency views')
            for dep_view_name in dependency_views:
                if dep_view_name not in self.athena._metadata.keys():
                    print(f'Missing dependency view: {dep_view_name}, creating')
                    logger.info(f'Missing dependency view: {dep_view_name}, creating')
                self.create_or_update_view(dep_view_name, recursive=recursive, update=update)
        view_query = self.get_view_query(view_name=view_name)
        logger.debug(f'view_query: {view_query}')
        if view_name in self.athena._metadata.keys():
            logger.debug(f'View "{view_name}" exists')
            if update:
                logger.info(f'Updating view: "{view_name}"')
                if view_definition.get('type') == 'Glue_Table':
                    print(f'Updating table {view_name}')
                    self.glue.create_or_update_table(view_name, view_query)
                else:
                    if 'CREATE EXTERNAL TABLE' in view_query.upper():
                        logger.warning('Cannot recreate table {view_name}')
                    elif 'CREATE OR REPLACE' in view_query.upper():
                        self.athena.create_or_update_view(view_name=view_name, view_query=view_query)
                    else:
                        print(f'View "{view_name}" is not compatible with update. Skipping.')
                if 'CREATE OR REPLACE VIEW' in view_query.upper() or 'CREATE VIEW' in view_query.upper():
                    logger.debug('Start waiting')
                    assert self.athena.wait_for_view(view_name), f"Failed to update a view {view_name}"
                    logger.info(f'View "{view_name}" updated')
        else: # No found -> creation
            logger.info(f'Creating view: "{view_name}"')
            if view_definition.get('type') == 'Glue_Table':
                self.glue.create_or_update_table(view_name, view_query)
                logger.info(f'Table "{view_name}" created')
            elif 'CREATE EXTERNAL TABLE' in view_query.upper():
                print(f'Creating table: "{view_name}"')
                try:
                    self.athena.execute_query(view_query)
                except CidCritical as exc:
                    logger.exception(exc)
                    pass
            else:
                self.athena.execute_query(view_query)
            assert self.athena.wait_for_view(view_name), f"Failed to create a view {view_name}"
            logger.info(f'View "{view_name}" created')

        if 'crawler' in view_definition:
            if not ('CREATE EXTERNAL TABLE' in view_query.upper() or view_definition.get('type') == 'Glue_Table'):
                raise CidCritical(f'Crawler cannot be defined for a view ({view_name}). only for a table. Pease fix resource definitions')
            location = self.glue.get_table(name=view_name, catalog=self.base.account_id, database=self.athena.DatabaseName).get('StorageDescriptor', {}).get('Location')
            self.create_or_update_crawler(crawler_name=view_definition['crawler'], location=location)

    def _confirm_dataset_experience_migration(self, found_dataset) -> bool:
        """Check if other dashboards use this dataset and ask the user whether to proceed
        with the new experience migration (delete/recreate). Returns True to proceed, False to skip."""
        current_dashboard_id = get_parameters().get('dashboard-id')
        other_dashboards = self.qs.find_dashboards_using_dataset(
            dataset_id=found_dataset.id,
            exclude_dashboard_ids=[current_dashboard_id] if current_dashboard_id else [],
        )

        if other_dashboards:
            dashboard_list = '\n'.join(
                f'  - {d["Name"]} ({d["DashboardId"]})'
                for d in other_dashboards
            )
            current_dashboard = get_parameters().get('dashboard-id', 'this dashboard')
            cid_print(
                f'<BOLD><RED>Warning:<END> <BOLD><RED>The following dashboards also use dataset '
                f'{found_dataset.name}:<END>\n{dashboard_list}\n\n'
                f'<BOLD><RED>Migrating to the new QuickSight Data Preparation Experience will delete and recreate '
                f'this dataset, temporarily breaking the dashboards listed above.<END>\n'
                f'We recommend proceeding, but update those dashboards immediately after updating <BOLD>{current_dashboard}<END>.\n'
                f'If you choose <BOLD>No<END>, the dataset will not be updated.'
            )
            return get_yesno_parameter(
                param_name=f'migrate-{found_dataset.name.replace("_", "-")}-new-experience',
                message=f'Proceed with new experience migration for {found_dataset.name}?',
                default='no',
            )
        else:
            return True



    def create_or_update_crawler(self, crawler_name, location):
        """ Create or Update Crawler """
        crawler_definition = self.get_definition("crawler", name=crawler_name)
        data = self.get_data_from_definition(crawler_definition)
        template = Template(json.dumps(data))

        # Filter roles trusted by Glue
        glue_trusted_roles = list(self.iam.iterate_role_names(search="Roles[?AssumeRolePolicyDocument.Statement[?Principal.Service == 'glue.amazonaws.com']].RoleName"))
        table = [fragment for fragment in location.split('/') if fragment][-1].lower().replace('-', '_')
        crawler_role = get_parameter(
            'crawler-role',
            message='Provide a crawler role name',
            choices=glue_trusted_roles + ['CidCmdCurCrawlerRole <CREATE NEW>']
        )
        if 'CREATE NEW' in crawler_role:
            crawler_role = 'CidCmdCurCrawlerRole'
            self.iam.ensure_role_for_crawler(
                s3bucket=location.split('/')[2],
                database=self.athena.DatabaseName,
                table=table,
                role_name=crawler_role
            )

        if not crawler_role.startswith('arn:'):
            crawler_role_arn = f"arn:{self.base.partition}:iam::{self.base.account_id}:role/{crawler_role}"
        else:
            crawler_role_arn = crawler_role
        params = {
            'athena_database_name': self.athena.DatabaseName,
            'crawler_role_arn': crawler_role_arn,
            'location': location,
        }
        compiled_definition = json.loads(template.safe_substitute(params))
        self.glue.create_or_update_crawler(crawler_definition=compiled_definition)


    def generic_tags_json(self, param_name='resource-tags', options=[]) -> str:
        ''' returns an sql for json tag
        '''
        def _tag_to_name(tag):
            if tag == 'line_item_iam_principal':
                return 'iam_principal'
            tag_name = (tag
                .replace('resource_tags_', '')
                .replace('cost_category_', '')
                .replace("'user_","'tag_")
                .replace('accountTag/', '')
                .replace('userAttribute/', '')
                .replace('iamPrincipal/', '')
                .replace("'aws_","'tag_aws_")
                .split("['")[-1].split("']")[0]
            )
            if not tag_name.startswith('tag_'):
                if tag.startswith('cost_category'):
                    tag_name = 'cost_category_' + tag_name
                elif "userAttribute/" in tag:
                    tag_name = 'user_attribute_tag_' + tag_name
                elif "iamPrincipal/" in tag:
                    tag_name = 'iam_principal_tag_' + tag_name
                elif tag.startswith('tags'):
                    tag_name = 'account_tag_' + tag_name
                else:
                    tag_name = 'tag_' + tag_name
            return re.sub(r'\W', '_', tag_name)

        resource_tags = get_parameters().get(param_name, None) or get_parameters().get(param_name.replace('_', '-'), None)
        tags_and_names = {_tag_to_name(tag):tag  for tag in sorted(options)}
        logger.info(f'tags_and_names = {tags_and_names}')
        logger.info(f'resource_tags = {resource_tags}')
        if isinstance(resource_tags, str):
            resource_tags = [tag for tag in resource_tags.split(',') if tag]
        if resource_tags is None:
            # group choices by their source (resource tag, IAM principal tag, cost category ...)
            grouper = TaxonomyMerger(tag_fields=sorted(set(tags_and_names.keys())))
            resource_tags = get_parameter(
                param_name,
                message='Select Cost Allocation Tags and Cost Categories to be added to datasets(WARNING: this can affect performance. Choose only the strict minimum)',
                multi=True,
                choices=grouper.grouped_choices(grouper.tag_fields),
                default=resource_tags or [],
            )

        if not resource_tags:
            return "'{}'"
        logger.debug(f'selected_tag_names = {resource_tags}')
        pattern = r"\W"
        array = ',\n                        '.join(
            # replace all special characters with _ to allow QS read from this json (QS parseJson does not like special characters)
            [f"""('{re.sub(pattern, "_", name)}', {tags_and_names[name]})"""
            for name in resource_tags]
        )
        res = f'''
            json_format(
                CAST (
                    MAP_FROM_ENTRIES (
                        ARRAY[
                            {array}
                        ]
                    )
                AS JSON)
            )
        '''
        logger.trace(f'cur_tags_json = {res}')
        return res

    def cur_tags_json(self, cur) -> str:
        return self.generic_tags_json(
            param_name='resource-tags',
            options=cur.tag_and_cost_category_fields,
        )

    def get_view_query(self, view_name: str) -> str:
        """ Returns a fully compiled AHQ """
        # View path
        view_definition = self.get_definition("view", name=view_name)
        cur1_required = view_definition.get('dependsOn', dict()).get('cur') or view_definition.get('dependsOn', dict()).get('cur1')
        cur2_required = view_definition.get('dependsOn', dict()).get('cur2')
        cur_tags_json_required = view_definition.get('dependsOn', dict()).get('tags') == 'json'

        #if cur_required and self.cur.has_savings_plans and self.cur.has_reservations and view_definition.get('spriFile'):
        #    view_definition['File'] = view_definition.get('spriFile')
        #elif cur_required and self.cur.has_savings_plans and view_definition.get('spFile'):
        #    view_definition['File'] = view_definition.get('spFile')
        #elif cur_required and self.cur.has_reservations and view_definition.get('riFile'):
        #    view_definition['File'] = view_definition.get('riFile')
        #if view_definition.get('File') or view_definition.get('Data') or view_definition.get('data'):
        #    pass
        #else:
        #    logger.critical(f'\nCannot find view {view_name}. View information is incorrect, please check resources.yaml')
        #    raise Exception(f'\nCannot find view {view_name}')

        # Load TPL file
        data = self.get_data_from_definition(view_definition)
        if isinstance(data, dict):
            template = Template(yaml.safe_dump(data))
        else:
            template = Template(data)

        # Prepare template parameters
        columns_tpl = {
            #'athena_datasource_arn': athena_datasource.arn,
            'athena_database_name': self.athena.DatabaseName,
            'athena_table_name': view_name,
            'cur_database':    self.cur1.database   if cur1_required else None, # for backward compatibly
            'cur_table_name':  self.cur1.table_name if cur1_required else None, # for backward compatibly
            'cur1_database':   self.cur1.database   if cur1_required else None,
            'cur1_table_name': self.cur1.table_name if cur1_required else None,
            'cur2_database':   self.cur2.database   if cur2_required else None,
            'cur2_table_name': self.cur2.table_name if cur2_required else None,
            'cur_tags_json':
                self.cur_tags_json(self.cur2 if cur2_required else self.cur1)
                if cur_tags_json_required
                else None,
        }

        columns_tpl = self.get_template_parameters(
            view_definition.get('parameters', dict()),
            f'view-{view_name}-',
            columns_tpl,
        )
        logger.debug(f"Rendering template for {view_name}")
        logger.debug(str(columns_tpl))
        columns_tpl = {key: str(value) for key, value in columns_tpl.items()}
        compiled_query = template.safe_substitute(columns_tpl)

        return compiled_query

    @command
    def csv2view(self, **kwargs):
        """CSV 2 SQL"""
        input_file = get_parameter('input', message='Enter csv filename')
        file_name = os.path.splitext(os.path.split(input_file)[-1])[0]
        name = get_parameter('name', message='Enter view name', default=file_name)
        csv2view(input_file, name)

    @command
    def map(self, **kwargs):
        """Create account mapping Athena views"""
        view_name = kwargs.get('view_name', 'account_map')
        
        # Use simple/legacy mode if --simple flag is provided
        if kwargs.get('simple'):
            print("\n🔄 Using simple account mapping (legacy mode)\n")
            return self.create_or_update_account_map(view_name)
        
        # Use advanced interactive mode (default)
        mapper = AccountMapper(athena=self.athena, view_name=view_name)
        
        try:
            mapper.create_mapping(
                source_file=kwargs.get('source_file'),
                source_database=kwargs.get('source_database'),
            )
        except Exception as e:
            logger.error(f"Account mapping failed: {e}", exc_info=True)
            print(f"\n❌ Error: {e}\n")
            raise

    @command
    def teardown(self, **kwargs):
        """remove all assets created by cid"""
        dashboards = list(self.qs.dashboards.values())
        cid_print('Following dashboards and datasets will be <RED><BOLD>deleted<END><END>:')
        for dashboard in dashboards:
            cid_print(f' <RED><BOLD>{dashboard.id}<END><END> <RED>{dashboard.name} <END>')

        if not get_yesno_parameter(param_name='confirm',
            message='You selected Teardown command. It will destroy ALL dashboards, datasets and datasources created by CID. Are you sure?',
            default='no'):
            cid_print('Good')
            return

        for dashboard in list(self.qs.dashboards.values()):
            self.delete(dashboard.id)
        self.iam.ensure_role_does_not_exist('CidCmdQuickSightDataSourceRole')
        self.iam.ensure_role_does_not_exist('CidCmdCurCrawlerRole')
        self.qs.delete_data_source('CID-CMD-Athena')

    @command
    def init_qs(self, **kwargs):
        """ Initialize QuickSight resources for deployment """
        return InitQsCommand(cid=self, **kwargs).execute()

    @command
    def create_cur_proxy(self, cur_version=None, fields=None, **kwargs):
        cid_print(f'Using CUR {self.cur.table_name}') # need to call self.cur
        cur_version = cur_version or get_parameter(
            'cur-version',
            message='Enter a version of CUR you want to create or update',
            choices=['1', '2'],
        )
        if cur_version.startswith('1'):
            cur_proxy = self.cur1
        if cur_version.startswith('2'):
            cur_proxy = self.cur2
        fields = get_parameters().get('fields', [])
        cur_proxy.metadata
        cur_proxy.proxy.fields_to_expose += (fields.split(',') if fields else [])
        cur_proxy.proxy.create_or_update_view()
        print('done')

    @command
    def create_cur_table(self, **kwargs):
        """ Initialize CUR """
        if get_parameters().get('view-cur-location'):
            s3path = get_parameters().get('view-cur-location')
        else:
            bucket = get_parameter(
                'view-cur-s3-bucket',
                message='Enter a bucket with CUR',
                choices=self.s3.list_buckets(),
            )
            s3path = get_parameter(
                'view-cur-location',
                message='Enter an S3 path. We support only 2 types of CUR path: s3://{bucket}/cur and s3://{bucket}/{prefix}/{name}/{name}',
                default=f's3://{bucket}/cur/',
            )
        path_fragments = [fragment for fragment in s3path.split('/')[3:] if fragment]

        if path_fragments == ['cur']: # our standard cur created by CID
            set_parameters({
                'view-cur-partitions': (
                    '[{"Name":"source_account_id","Type":"string"},'
                    '{"Name":"cur_name_1","Type":"string"},'
                    '{"Name":"cur_name_2","Type":"string"},'
                    '{"Name":"year","Type":"string"},'
                    '{"Name":"month","Type":"string"}]'
                )
            })
        elif len(path_fragments) == 3 and path_fragments[-1] == path_fragments[-2]: # CUR that is not created by CID but still supported
            set_parameters({'view-cur-partitions': '[{"Name":"year","Type":"string"},{"Name":"month","Type":"string"}]'})
        else:
            raise NotImplementedError(f"We support only 2 types of CUR and this is something else ({s3path}).")

        set_parameters({'crawler-cur-s3path': get_parameters().get('view-cur-location')})
        cid_print('Creating / updating CUR table')
        self.create_or_update_view(view_name='cur')
        cid_print('Please check crawler in a few minutes https://console.aws.amazon.com/glue/home?#/v2/data-catalog/crawlers')
        set_parameters({'cur-table-name': path_fragments[-1].lower().replace('-', '_')})
