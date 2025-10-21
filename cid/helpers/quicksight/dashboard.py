import io
import json
import logging
from typing import Dict

import yaml

from cid.helpers.quicksight.resource import CidQsResource
from cid.helpers.quicksight.definition import Definition as CidQsDefinition
from cid.helpers.quicksight.template import Template as CidQsTemplate
from cid.utils import cid_print, get_yesno_parameter
from cid.helpers.quicksight.resource import CidQsResource
from cid.helpers.quicksight.dataset import Dataset
from cid.helpers.quicksight.version import CidVersion


logger = logging.getLogger(__name__)


class Dashboard(CidQsResource):
    def __init__(self, raw: dict, qs=None) -> None:
        super().__init__(raw)
        # Initialize properties
        self._datasets: Dict[str, str] = {}
        self._definition: Dict = None  # CID definition from resource yaml. THIS IS NOT QS DEFINITION!
        self._tag_version: str = None
        self._deployed_template: CidQsTemplate = None
        self._deployed_definition: CidQsDefinition = None
        self._status: str = ''
        self.status_detail: str = ''
        self._source_template: CidQsTemplate = None
        self._source_definition: CidQsDefinition = None
        self._cid_version = None
        self.qs = qs

    @property
    def id(self) -> str:
        '''DashboardId'''
        return self.get_property('DashboardId')


    @property
    def arn(self) -> str:
        '''Arn'''
        return self.get_property('Arn').split('/version/')[0]


    @property
    def version(self) -> dict:
        '''dashboard's data in the current version. Please note it is not cid version'''
        return self.get_property('Version')


    @property
    def definition(self):
        ''' CID definition from resource yaml. THIS IS NOT QS DEFINITION!
        '''
        if self._definition is not None:
            return self._definition
        # Look for dashboard definition by DashboardId in the catalog of supported dashboards (the currently available definitions in their latest public version)
        # This definition can be used to determine the gap between the latest public version and the currently deployed version
        self._definition = next((v for v in self.qs.supported_dashboards.values() if v['dashboardId'] == self.id), None)

        if not self._definition:
            # Look for dashboard definition by templateId.
            # This is for a specific use-case when a dashboard with another id points to managed template
            logger.debug(f'dashboard "{self.id} is not found in supported dashboards by id, will try to match by template."')
            source_arn = self.raw.get('Version', {}).get('SourceEntityArn', '')
            if source_arn:
                template_id = source_arn.split('/version/')[0].split('/')[-1]
                template_account = source_arn.split(':')[4]
                self._definition = next((v for v in self.qs.supported_dashboards.values() if 'templateId' in v and v['templateId'] == template_id), None)
        if not self._definition:
            self._definition = {}
            logger.info(f'Unsupported dashboard "{self.name}"')
        return self._definition


    @property
    def source_template(self) -> CidQsTemplate:
        # Fetch the latest version of source_template referenced in definition
        if self._source_template:
            return self._source_template
        template_id = self.definition.get('templateId')
        if template_id:
            source_template_account_id = self.definition.get('sourceAccountId')
            region = self.definition.get('region', 'us-east-1')
            try:
                logger.debug(f'Loading latest source template {template_id} from source account {source_template_account_id} in {region}')
                self._source_template = self.qs.describe_template(
                    template_id,
                    account_id=source_template_account_id,
                    region=region
                )
            except Exception as exc:
                logger.debug(exc, exc_info=True)
                logger.info(f'Unable to describe template {template_id} in {source_template_account_id} ({region})')
        return self._source_template

    @property
    def deployed_template(self) -> CidQsTemplate:
        ''' Fetch template referenced as current dashboard source (if any)
        '''
        if self._deployed_template:
            return  self._deployed_template
        _template_arn = self.version.get('SourceEntityArn')

        if _template_arn and isinstance(_template_arn, str) \
            and len(_template_arn.split(':')) > 5 \
            and _template_arn.split(':')[5].startswith('template/'):
            params = {
                "region": _template_arn.split(':')[3],
                "account_id": _template_arn.split(':')[4],
                "template_id": _template_arn.split('/')[1],
            }
            if '/version/' in _template_arn:
                params['version_number'] = int(_template_arn.split('/version/')[-1] or 0)
            else:
                # in some older deployments versions was not referenced so we try to get it from resources yaml
                version_obj = self.definition.get('versions', {}) if self.definition else {}
                min_template_version = int(version_obj.get('minTemplateVersion', 0)) # 0 is not a valid version for template. it starts with 1
                if min_template_version:
                    logger.debug(f"Using default version number {min_template_version} in place")
                    params['version_number'] = min_template_version
                else:
                    logger.debug("Minimum template version could not be found for Dashboard {self.id}: {_template_arn}. We cannot describe deployed template and get the version.")
                    return self._deployed_template # None
            try:
                logger.debug(f'Describing template {_template_arn}')
                _template = self.qs.describe_template(**params)
                if isinstance(_template, CidQsTemplate):
                    self._deployed_template = _template
            except Exception as exc:
                logger.debug(exc, exc_info=True)
                logger.debug(f'Unable to describe template for {self.id}, {exc}')
        return self._deployed_template

    @deployed_template.setter
    def deployed_template(self, template: CidQsTemplate) -> None:
        self._deployed_template = template

    @property
    def deployed_definition(self):
        if not self._deployed_definition:
            self._deployed_definition = self.qs.describe_dashboard_definition(dashboard_id=self.id, refresh=True)
        return self._deployed_definition

    @deployed_definition.setter
    def deployed_definition(self, definition: CidQsDefinition) -> None:
        self._deployed_definition = definition

    @property
    def template_id(self) -> str:
        if isinstance(self.deployed_template, CidQsTemplate):
            return self.deployed_template.id
        return None

    @property
    def template_arn(self) -> str:
        if isinstance(self.deployed_template, CidQsTemplate):
            return self.deployed_template.arn
        return None


    def get_dataset_ids(self):
        return [dataset.split('/')[-1] for dataset in self.version.get('DataSetArns', [])]

    @property
    def datasets(self):
        if self._datasets:
            return self._datasets
        for dataset_id in self.get_dataset_ids():
            try:
                _dataset = self.qs.describe_dataset(id=dataset_id)
                if not isinstance(_dataset, Dataset):
                    logger.debug(f'Dataset "{dataset_id}" is missing')
                else:
                    logger.trace(f"Detected dataset: \"{_dataset.name}\" ({_dataset.id} in {self.id})")
                    self._datasets[_dataset.name] =  _dataset.id
            except self.qs.client.exceptions.AccessDeniedException:
                logger.debug(f'Access denied describing DataSetId {dataset_id} for Dashboard {self.id}')
            except self.qs.client.exceptions.InvalidParameterValueException:
                logger.debug(f'Invalid dataset {dataset_id}')
        logger.info(f"{self.name} has {len(self._datasets)} datasets")
        return self._datasets

    @property
    def views(self):
        # Fetch all views recursively
        all_views = []
        def _recursive_add_view(view):
            all_views.append(view)
            for dep_view in (self.qs.supported_views.get(view) or {}).get('dependsOn', {}).get('views', []):
                _recursive_add_view(dep_view)
        for dataset_name in self.datasets or []:
            for view in (self.qs.supported_datasets.get(dataset_name) or {}).get('dependsOn', {}).get('views', []):
                _recursive_add_view(view)
        return all_views


    @property
    def latest(self) -> bool:
        try:
            return self.latest_available_cid_version == self.deployed_cid_version
        except Exception as exc:
            logger.debug(f'Failed to determine if latest for dashboards: {self.id}. {exc}')
            return None

    @property
    def health(self) -> bool:
        return self.status not in ['broken']

    @property
    def deployed_cid_version(self):
        if self._cid_version:
            return  self._cid_version
        tag_version = (self.qs.get_tags(self.arn) or {}).get('cid_version_tag')
        #print(f'{self.id}: {tag_version}')
        if tag_version:
            logger.trace(f'version of {self.arn} from tag = {tag_version}')
            self._cid_version = CidVersion(tag_version)
        else:
            if self.deployed_template:
                self._cid_version = self.deployed_template.cid_version
            elif self.deployed_definition:
                self._cid_version = self.deployed_definition.cid_version
            if self._cid_version:
                logger.trace(f'setting tag of {self.arn} to cid_version_tag = {self._cid_version}')
                self.qs.set_tags(self.arn, cid_version_tag=self._cid_version)
        return self._cid_version


    @property
    def cid_version(self): # for backward compatibility
        return self.deployed_cid_version


    @property
    def latest_available_cid_version(self) -> int:
        if 'version' in self._definition:
            return CidVersion(self._definition['version'])

        if self.source_template:
            return self.source_template.cid_version
        elif self.source_definition:
            return self.source_definition.cid_version
        else:
            return None

    @property
    def supported(self) -> bool:
        return True if self.definition else False 


    @property
    def source_definition(self):
        if self._source_definition:
            return self._source_definition
        if 'data' in self.definition:
            # Resolve source definition (the latest definition publicly available)
            data_stream = io.StringIO(self.definition["data"])
            definition_data = yaml.safe_load(data_stream) # FIXME: there can be template variables. 
            self._source_definition = CidQsDefinition(definition_data)
        return self._source_definition

    @property
    def status(self) -> str:
        if not self._status:
            # Deployment failed
            if self.version.get('Status') not in ['CREATION_SUCCESSFUL']:
                self._status = 'broken'
                self.status_detail = f"{self.version.get('Status')}: {self.version.get('Errors')}"
            # Not discovered yet
            elif not self.definition:
                self._status = 'undiscovered'
            # Source Template has changed
            elif self.deployed_template and self.source_template and self.deployed_template.arn and self.source_template.arn and not self.deployed_template.arn.startswith(self.source_template.arn):
                self._status = 'legacy'
            elif not self.latest_available_cid_version or not self.deployed_cid_version:
                self._status = 'undetermined'
            else:
                if self.latest_available_cid_version > self.deployed_cid_version:
                    self._status = f'update available {self.deployed_cid_version}->{self.latest_available_cid_version}'
                elif self.latest:
                    self._status = 'up to date'
        return self._status

    def display_status(self) -> None:
        """Display status of dashboard"""

        cid_print('\n<BOLD>Dashboard status:<END>')
        cid_print(f"  <BOLD>Id:<END>        {self.id}")
        cid_print(f"  <BOLD>Name:<END>      {self.name}")
        cid_print(f"  <BOLD>Health:<END>    {'<GREEN>healthy<END>' if self.health else '<RED>unhealthy<END>'}")
        cid_print(f"  <BOLD>Status:<END>    {'<GREEN>' + self.status + '<END>' if self.health else '<RED>' + self.status + '<END>'}")

        if self.status_detail:
            cid_print(f"  <BOLD>Status detail:<END> {self.status_detail}")

        if not self.cid_version:
            logger.debug("The cid version of the deployed dashboard could not be retrieved")

        if not self.latest_available_cid_version:
            logger.debug("The latest version of the dashboard could not be retrieved")
            cid_print(f"  <BOLD>Version:<END>   <YELLOW>{self.cid_version or 'N/A'}<END> (unable to find latest)")
        else:
            if self.latest:
                cid_print(f"  <BOLD>Version:<END>   <GREEN>{self.cid_version or 'N/A'}<END> (latest)")
            else:
                logger.debug("An update is available")
                cid_print(f"  <BOLD>Version:<END>   <YELLOW>{self.cid_version or 'N/A'} --> {self.latest_available_cid_version or 'N/A'}<END>")

        cid_print('  <BOLD>Owners:<END>')
        try:
            permissions = self.qs.get_dashboard_permissions(self.id)
            for permission in permissions:
                if 'quicksight:UpdateDashboardPermissions' in permission["Actions"]:
                    cid_print('    ' + permission["Principal"].split('user/default/')[-1])
        except Exception as exc:
            if "AccessDenied" in str(exc):
                cid_print('     <RED>AccessDenied<END>')

        if self.datasets:
            cid_print(f"  <BOLD>Datasets:<END>")
            for dataset_name, dataset_id in  sorted(self.datasets.items()):
                ds = self.qs.describe_dataset(dataset_id)
                rls = {'ENABLED': '🔐', 'DISABLED': '🔓', None: ' '}.get(ds.rls_status)
                status = self.qs.get_dataset_last_ingestion(dataset_id) or '<BLUE>DIRECT<END>' #todo fix this Blue using dataset import type.
                cid_print(f'   {rls} {dataset_name: <36} ({dataset_id: <36}) {status}')

    def display_url(self, url_template: str, launch: bool = False, **kwargs) -> None:
        url = url_template.format(dashboard_id=self.id, **kwargs)
        print(f"#######\n####### {self.name} is available at: " + url + "\n#######")

    def refresh_datasets(self) -> None:
        """Refresh datasets of dashboard"""
        if self.datasets:
            cid_print(f"  <BOLD>Refreshing Datasets:<END>")
            for dataset_name, dataset_id in  sorted(self.datasets.items()):
                mode, status = self.qs.refresh_dataset(dataset_id)
                cid_print(f'    {dataset_name: <36} ({dataset_id: <36}) Refresh Status: {status} Mode: {mode}')
