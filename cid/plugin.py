# Implements generic Plugin class to load plugins

import json, yaml
from importlib import resources
import logging

logger = logging.getLogger(__name__)

class Plugin():

    def __init__(self, name):
        logger.debug(f'Initializing plugin {name}')
        self.resources = {}
        self.name = name
        data_directory = 'data'
        data_path = resources.files(self.name) / data_directory
        if data_path.is_dir():
            for resource_file in data_path.iterdir():
                if resource_file.is_file():
                    logger.debug(f'Located data file: {resource_file.name}')
                    ext = resource_file.name.rsplit('.', -1)[-1].lower()
                    content = None
                    if ext == 'json':
                        content = json.loads(resource_file.read_text())
                        logger.debug(f'Loaded {resource_file.name} as JSON')
                    elif ext in ['yaml', 'yml']:
                        content = yaml.load(resource_file.read_text(), Loader=yaml.SafeLoader)
                        logger.debug(f'Loaded {resource_file.name} as YAML')
                    if content is None:
                        logger.info(f'Unsupported file type: {resource_file.name}')
                        continue
                    # If plugin has resources defined in different files,
                    # they will be merged into one dict
                    resource_kind = resource_file.name.rsplit('.', -1)[0]
                    supported_resource_kinds = ['dashboards', 'views', 'datasets']
                    if resource_kind in supported_resource_kinds:
                        self.resources[resource_kind] = content
                        logger.info(f'Loaded {resource_kind} from {resource_file.name}')
                    # If plugin has resources defined in one file,
                    # simply add it to resources dict
                    else:
                        self.resources.update(content)
                    # Add plugin name to every resource
                    for v in self.resources.values():
                        for item in v.values():
                            if item is not None:
                                item['providedBy'] = self.name
                                item.update({'source': str(resource_file)})
        logger.debug(f'Plugin {self.name} initialized')
    
    def provides(self) -> dict:
        logger.debug(f'Provides: {self.resources}')
        return self.resources

    def get_resource(self, resource_name) -> str:
        _resource_path = resources.files(self.name) / f'data/{resource_name}'
        if _resource_path.is_file():
            logger.info(f'Resource {resource_name} found')
            _content = _resource_path.read_text()
            logger.debug(f'Resource {resource_name} content: {_content}')
            return _content
        return None
