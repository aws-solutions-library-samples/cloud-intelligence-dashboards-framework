''' This module analyses Quicksight Analysis and pulls the definitions of dependencies. 

Here are the types of objects that are processed:

    * QuickSight Analysis
    * QuickSight DataSets
    * Athena Views
    * Glue Tables
    * Glue Crawlers

'''
import os
import re
import time
import logging

import yaml
import boto3

from cid.helpers import Dataset, QuickSight, Athena, Glue, CUR
from cid.helpers.quicksight.definition import Definition
from cid.helpers.quicksight.dashboard_patching import remove_fields, detect_global_filter_fields
from cid.utils import get_parameter, get_parameters, cid_print, get_defaults
from cid.exceptions import CidCritical

logger = logging.getLogger(__name__)

def enable_multiline_in_yaml():
    """ Enable multiline in yaml

    credits: https://stackoverflow.com/a/33300001
    """
    def str_presenter(dumper, data):
        if len(data.splitlines()) > 1:  # check for multiline string
            data =  re.sub(r'\s+$', '', data, flags=re.M) # Multiline does not support traling spaces
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)
    yaml.add_representer(str, str_presenter)
    yaml.representer.SafeRepresenter.add_representer(str, str_presenter) # to use with safe_dump

def escape_id(id_):
    """ Escape id """
    return re.sub('[^0-9a-zA-Z]+', '-', id_)

def choose_analysis(qs):
    """ Choose analysis """
    try:
        analyzes =  qs.client.get_paginator('list_analyses').paginate(AwsAccountId=qs.account_id).search('AnalysisSummaryList')
        analyzes = list(filter(lambda a: a['Status']=='CREATION_SUCCESSFUL', analyzes)) # get only correct ones
    except qs.client.exceptions.AccessDeniedException:
        logger.info("AccessDeniedException while discovering analyses")
        return None

    if not analyzes:
        raise CidCritical("No analyses was found, please save your dashboard as an analyse first")
    choices = {a['Name']:a for a in sorted(analyzes, key=lambda a: a['LastUpdatedTime'])[::-1]}
    choice = get_parameter(
        'analysis-name',
        message='Select Analysis you want to share.',
        choices=list(choices.keys()),
    )
    return choices[choice]['AnalysisId']


def get_theme(analysis):
    theme_arn = analysis.get('ThemeArn')
    if not theme_arn:
        return None
    if theme_arn.startswith('arn:aws:quicksight::aws:theme/'):
        return theme_arn.split('/')[-1]
    logger.warning(f'Theme {theme_arn} is not standard and is not supported yet. Theme will be ignored.')
    return None


def export_analysis(qs, athena, glue):
    """ Export analysis to yaml resource File
    """

    enable_multiline_in_yaml()
    # Choose Analysis to share
    analysis_id = get_parameters().get('analysis-id') or choose_analysis(qs)

    if not analysis_id:
        analysis_id = get_parameter(
            'analysis-id',
            message='Enter ID of analysis you want to share (open analysis in browser and copy id from url or the full url of the analysis)',
        )
    if not analysis_id:
        raise CidCritical("Need a parameter --analysis-id or --analysis-name")

    if analysis_id.startswith('https://') and '/analyses/' in analysis_id:
        analysis_id = analysis_id.split('/analyses/')[1].split('/')[0]

    analysis = qs.client.describe_analysis(
        AwsAccountId=qs.account_id,
        AnalysisId=analysis_id
    )['Analysis']

    logger.info("analyzing datasets")
    resources = {}

    output = get_parameter(
        'output',
        message='Enter a filename (.yaml)',
        default=f"{analysis['Name'].replace(' ', '-')}.yaml"
    )
    if os.path.exists(output):
        try:
            resources = yaml.safe_load(open(output).read())
        except Exception as exc:
            logger.warning(f'Error loading {output}: {exc}. Will continue.')

    resources['dashboards'] = resources.get('dashboards') or {}
    resources['datasets'] = resources.get('datasets') or {}
    resources['crawlers'] = resources.get('crawlers') or {}

    if resources['dashboards']:
        dashboard_key = list(resources['dashboards'].keys())[0] # not supported multi dashboards
    else:
        dashboard_key = analysis['Name'].upper()

    cur_helper = CUR(athena=athena, glue=glue)

    resources_datasets = []

    dataset_references = []
    datasets = {}
    all_views_and_databases = []
    for dataset_arn in analysis['DataSetArns']:
        dependency_views = []
        dataset_id = dataset_arn.split('/')[-1]
        dataset = qs.describe_dataset(dataset_id)

        if not isinstance(dataset, Dataset):
            raise CidCritical(f'dataset {dataset_id} not found. '
                'We need all datasets to be preset for template generation')

        dataset_name = dataset.raw['Name']

        dataset_references.append({
            "DataSetPlaceholder": dataset_name,
            "DataSetArn": dataset_arn
        })

        cid_print(f'    Found DataSet <BOLD>{dataset_name}<END>.')
        if dataset_name in athena._resources.get('datasets'):
            resources_datasets.append(dataset_name) # used later in dependencies
            if not get_parameters().get('export-known-datasets') == 'yes':
                cid_print(f'    DataSet <BOLD>{dataset_name}<END> is in resources. Skipping.')
                continue # prevent export dataset

        dataset_data = {
            "DataSetId": dataset.raw['DataSetId'],
            "Name": dataset.raw['Name'],
            "PhysicalTableMap": dataset.raw['PhysicalTableMap'],
            "LogicalTableMap": dataset.raw['LogicalTableMap'],
            "ImportMode": dataset.raw['ImportMode'],
        }

        for key, value in dataset_data['PhysicalTableMap'].items(): # iterate all sub tables
            if 'RelationalTable' in value \
                and 'DataSourceArn' in value['RelationalTable'] \
                and 'Schema' in value['RelationalTable']:
                logger.debug(f"Dataset {dataset.raw['DataSetId']} looks like classic athena dataset")
                value['RelationalTable']['DataSourceArn'] = '${athena_datasource_arn}'
                database_name = value['RelationalTable']['Schema']
                discovered_databases = list(set([d for _, d in all_views_and_databases])) # default database is the first one
                if not discovered_databases or discovered_databases[0] == database_name:
                    value['RelationalTable']['Schema'] = '${athena_database_name}'
                else:
                    logger.warning('Database {database_name} is not default, you will need to make it a parameter.') #TODO: can be added on code level
                athena_source = value['RelationalTable']['Name']
                views_name = athena_source.split('.')[-1]
                dependency_views.append(views_name)
                if views_name in athena._resources.get('views') and not get_parameters().get('export-known-datasets'):
                    cid_print(f'    Athena view <BOLD>{views_name}<END> is in resources. Skipping')
                else:
                    all_views_and_databases.append((views_name, database_name))
            elif 'CustomSql' in value and 'DataSourceArn' in value['CustomSql']:
                logger.debug(f"Dataset {dataset.raw['DataSetId']} looks like CustomSql athena dataset")
                value['CustomSql']['DataSourceArn'] = '${athena_datasource_arn}'
                databases = athena.list_databases()
                for database in databases:
                    if f'{database}.' in value['CustomSql']['SqlQuery'] or f'"{database}".' in value['CustomSql']['SqlQuery']:
                        logger.debug(f"Replacing {database} in text")
                        value['CustomSql']['SqlQuery'] = value['CustomSql']['SqlQuery'].replace(database, '${athena_database_name}')
                logger.warning(f"Dataset {dataset.raw['DataSetId']} use SqlQuery. Discovery of tables and views for that is not supported yet. Please add them manually")
            else:
                raise CidCritical(f"Dataset {key} {dataset.raw['Name']} do not seems to be Athena dataset. Only Athena datasets are supported." )

        for key, value in dataset_data.get('LogicalTableMap', {}).items():
            if 'Source' in value and "DataSetArn" in value['Source']:
                #FIXME add value['Source']['DataSetArn'] to the list of dataset_arn
                raise CidCritical(f"DataSet {dataset.raw['Name']} contains unsupported join. Please replace join of {value.get('Alias')} from DataSet to DataSource")

        # Checking if datasets is based on CUR. It is rather are rare case as typically views depend on CUR.
        cur_version = False
        cur_fields = None
        for dep_view in dependency_views[:]:
            version = cur_helper.table_is_cur(name=dep_view)
            if version:
                dependency_views.remove(dep_view)
                cur_version = version
                cur_fields = True # FIXME: check the list of fields in datasets and fields in cur. Put a list instead of 'cur2: True'

        datasets[dataset_name] = {
            'data': dataset_data,
            'dependsOn': {'views': dependency_views},
            'schedules': ['default'], #FIXME: need to read a real schedule
        }

        if cur_version == '1':
            datasets[dataset_name]['dependsOn']['cur'] = cur_fields
        elif cur_version == '2':
            datasets[dataset_name]['dependsOn']['cur2'] = cur_fields

    all_views =     [view_and_database[0] for view_and_database in all_views_and_databases]
    all_databases = [view_and_database[1] for view_and_database in all_views_and_databases]
    if len(all_databases) > 1:
        logger.warning(f'CID only supports one database. Multiple used: {all_databases}')

    if all_databases:
        athena.DatabaseName = all_databases[0]

    cid_print(f'Analyzing Athena Views: <BOLD>{repr(all_views)}<END>. Can take some time.')
    all_views_data = athena.process_views(all_views)

    # Post processing of views:
    # - Special treatment for CUR: replace cur table with a placeholder
    # - Detect S3 paths as variables
    resources['views'] = {}
    cur_tables = []
    for key, view_data in all_views_data.items():
        if all_databases and isinstance(view_data.get('data'), str):
            view_data['data'] = view_data['data'].replace(f'{all_databases[0]}.', '"${athena_database_name}".')

        if isinstance(view_data.get('data'), str):
            view_data['data'] = view_data['data'].replace('CREATE VIEW ', 'CREATE OR REPLACE VIEW ')

        # Analyse dependencies: if the dependency is CUR there is a special flag
        deps = view_data.get('dependsOn', {})
        non_cur_dep_views = []
        for dep_view in deps.get('views', []):
            if '.' in dep_view:
                dep_view_name = dep_view.split('.')[1].replace('"','')
                dep_view_database = dep_view.split('.')[0].replace('"','')
            else:
                dep_view_name = dep_view
                dep_view_database = athena.DatabaseName

            if dep_view_name in cur_tables or cur_helper.table_is_cur(name=dep_view_name, database=dep_view_database):
                cur_helper.set_cur(table=dep_view_name, database=dep_view_database)
                cid_print(f'    {dep_view_name} is CUR {cur_helper.version}')
                # replace cur table name with a variable
                if isinstance(view_data.get('data'), str):
                    # cur tables treated separately as we don't manage CUR table here
                    cur_replacement = {
                        '2': ["${cur2_database}","${cur2_table_name}"],
                        '1': ["${cur_database}","${cur_table_name}"],
                    }[cur_helper.version]
                    view_data['data'] = re.sub(r'\b' + re.escape(dep_view) + r'\b', cur_replacement[1], view_data['data'])
                    view_data['data'] = re.sub(r'\b' + re.escape(dep_view_database) + r'\b', cur_replacement[0], view_data['data'])
                fields = []
                for field in cur_helper.fields:
                    if field in view_data['data']:
                        if field == 'data':
                            # 'data' is a partition of cur2 and a frequent word, so skip it to avoid false positives
                            continue
                        fields.append(field)
                view_data['dependsOn'][f'cur{cur_helper.version}'] = fields or True
                cur_tables.append(dep_view_name)
            else:
                dep_view_data = all_views_data.get(dep_view_name, {}).get('data', '')
                if 'CREATE EXTERNAL TABLE' in dep_view_data and not get_parameters().get('export-tables') == 'yes':
                    cid_print(f'{key} is a Glue table. Skipping. To export table definitions use `--export-tables yes`. Please manage this dependency by adding parameter')
                    # TODO: add parameter with schema query
                    continue
                logger.debug(f'{dep_view_name} is not cur')
                if dep_view_name not in all_views_data:
                    logger.debug(f'{dep_view_name} skipping as not in the views list')
                    continue
                non_cur_dep_views.append(dep_view_name)
        if deps.get('views'):
            deps['views'] = non_cur_dep_views
            if not deps['views']:
                del deps['views']

    logger.debug(f'cur_tables = {cur_tables}')
    # Add Views to Resources
    for key, view_data in all_views_data.items():
        if key in cur_tables or cur_helper.table_is_cur(name=key):
            logger.debug(f'Skipping {key} views - it is a CUR')
            continue
        if key in ['account_map']:
            cid_print(f'{key} is a special data. Processed separately. Skipping.')
            continue
        if isinstance(view_data.get('data'), str):
            if 'CREATE EXTERNAL TABLE' in view_data.get('data') and not get_parameters().get('export-tables') == 'yes':
                cid_print(f'{key} is a Glue table. Skipping. To export table definitions use `--export-tables yes`')
                continue
            #check if there is dependency on crawler
            crawler_names = re.findall(r"UPDATED_BY_CRAWLER\W+?'(.+?)''", view_data.get('data'))
            if crawler_names:
                crawler_name = crawler_names[0]
                crawler = {'data': glue.get_crawler(crawler_name)}

                # Post processing
                for crawler_key in crawler['data'].keys():
                    if crawler_key not in (
                        'Name', 'Description', 'Role', 'DatabaseName', 'Targets',
                        'SchemaChangePolicy', 'RecrawlPolicy', 'Schedule', 'Configuration'
                        ): # remove all keys that are not needed
                        crawler['data'].pop(crawler_key, None)
                if 'Schedule' in crawler['data']['Schedule']:
                    crawler['data']['Schedule'] = crawler['data']['Schedule']['ScheduleExpression']
                crawler['data']['Role'] = '${crawler_role_arn}'
                crawler['data']['DatabaseName'] = "${athena_database_name}"
                for index, target in enumerate(crawler['data'].get('Targets', [])):
                    path = target.get('Path')
                    default = get_parameter(
                        f'{key}-s3path',
                        'Please provide default value. (You can use {account_id} variable if needed)',
                        default=re.sub(r'(\d{12})', '{account_id}', path),
                        template_variables={'account_id': '{account_id}'},
                    )
                    crawler['parameters'] = { #FIXME: path should be the same as for table
                        's3path': {
                            'default': default,
                            'description': f"S3 Path for {key} table",
                        }
                    }
                    crawler['data']['Targets'][index]['Path'] = '${s3path}'
                crawler_name = key
                resources['crawlers'][crawler_name] = crawler
                view_data['crawler'] = crawler_name


            # replace location with variables
            locations = re.findall(r"LOCATION\W+?'(s3://.+?)'", view_data.get('data'))
            for location in locations:
                logger.info(f'Please replace manually location bucket with a parameter: {location}')
                default = get_parameter(
                    f'{key}-s3path',
                    'Please provide default value. (You can use {account_id} variable if needed)',
                    default=re.sub(r'(\d{12})', '{account_id}', location),
                    template_variables={'account_id': '{account_id}'},
                )
                view_data['parameters'] = {
                    's3path': {
                        'default': default,
                        'description': f"S3 Path for {key} table",
                    }
                }
                view_data['data'] = view_data['data'].replace(location, '${s3path}')

            if re.findall(r"PARTITION", view_data.get('data')) and 'crawler' not in view_data:
                logger.warning(f'The table {key} is partitioned but there no crawler info please make sure partitions are managed some way after install.')

        resources['views'][key] = view_data

    logger.debug('Building dashboard resource')
    dashboard_resource = resources.get('dashboards', {}).get(dashboard_key) or {}


    dashboard_id = dashboard_resource.get('dashboardId') or get_parameter(
        'dashboard-id',
        message='dashboard id (will be used in dashboard URL. Use lowercase, hyphens(not underscores) and make it short but understandable for humans)',
        default=escape_id(analysis['Name'].lower().replace(' ', '-').replace('_', '-'))
    )
    new_dashboard_id = dashboard_id.lower().replace(' ', '-').replace('_', '-')
    if dashboard_id != new_dashboard_id:
        cid_print(f'Best practices enforced: {dashboard_id} -> {new_dashboard_id}')
        dashboard_id = new_dashboard_id

    dashboard_resource['dependsOn'] = {
        # Historically CID uses dataset names as dataset reference. IDs of manually created resources have uuid format.
        # We can potentially reconsider this and use IDs at some point
        'datasets': sorted(list(set(list(datasets.keys()) + resources_datasets)))
    }
    dashboard_resource['name'] = dashboard_resource.get('name') or analysis['Name']
    dashboard_resource['dashboardId'] = dashboard_id
    dashboard_resource['category'] = get_parameters().get('category', dashboard_resource.get('category', 'Custom'))

    theme_id = get_theme(analysis)
    if theme_id:
         dashboard_resource['theme'] = theme_id

    dashboard_export_method = None
    if get_parameters().get('template-id') or dashboard_resource.get('templateId'):
        dashboard_export_method = 'template'
    elif dashboard_resource.get('file') or dashboard_resource.get('data'):
        dashboard_export_method = 'definition'
    else:
        dashboard_export_method = get_parameter(
            'dashboard-export-method',
            message='Please choose export method',
            choices={
                '[template]   Generate a QuickSight Template in the current account (Recommended)': 'template',
                '[definition] Save QuickSight Dashboard Definition in the file': 'definition',
            },
        )
    if dashboard_export_method == 'template':
        template_id = get_parameter(
            'template-id',
            message='Enter template id',
            default=escape_id(analysis['Name'].lower())
        )
        default_description_version = 'v0.0.1'
        try:
            old_template = qs.client.describe_template(
                AwsAccountId=qs.account_id,
                TemplateId=template_id,
            )['Template']
            default_description_version = old_template.get('Version', {}).get('Description')
        except qs.client.exceptions.ResourceNotFoundException:
            logger.debug('No previous template')
        template_version_description = get_parameter(
            'template-version-description',
            message='Enter version description',
            default=default_description_version, # FIXME: get version from analysis / template
        )

        logger.info('Updating template')
        params = {
            "AwsAccountId": qs.account_id,
            "TemplateId": template_id,
            "Name": template_id,
            "SourceEntity": {
                "SourceAnalysis": {
                    "Arn": analysis.get("Arn"),
                    "DataSetReferences": dataset_references
                }
            },
            "VersionDescription": template_version_description,
        }
        logger.debug(f'Template params = {params}')
        try:
            res = qs.client.update_template(**params)
            logger.info(f'Template {template_id} updated from Analysis {analysis.get("Arn")}')
        except qs.client.exceptions.ResourceNotFoundException:
            res = qs.client.create_template(**params)
            logger.info(f'Template {template_id} created from Analysis {analysis.get("Arn")}')

        if res['CreationStatus'] not in ['CREATION_IN_PROGRESS', 'UPDATE_IN_PROGRESS']:
            raise CidCritical(f'failed template operation {res}')

        template_arn = res['Arn']
        logger.info(f'Template arn = {template_arn}')


        reader_account_id = get_parameter(
            'reader-account',
            message='Enter account id to share the template with or *',
            default='*'
        )

        time.sleep(5) # Some times update_template_permissions does not work immediately.

        res = qs.update_template_permissions(
            TemplateId=template_id,
            GrantPermissions=[
                {
                    "Principal": f'arn:{qs.partition}:iam::{reader_account_id}:root' if reader_account_id != '*' else '*',
                    'Actions': [
                        "quicksight:DescribeTemplate",
                    ]
                },
            ],
        )
        dashboard_resource['templateId'] = template_id
        dashboard_resource['sourceAccountId'] = qs.account_id
        dashboard_resource['region'] = qs.session.region_name

    elif dashboard_export_method == 'definition':
        definition = qs.client.describe_analysis_definition(
            AwsAccountId=qs.account_id,
            AnalysisId=analysis_id,
        )['Definition']

        taxonomy_fields = detect_global_filter_fields(definition)
        taxonomy_fields = get_parameter('taxonomy',
            message='Enter the fields that you want removed from the taxonomy before export',
            choices=taxonomy_fields,
            default=[],
            multi=True
        )
        if taxonomy_fields:
            cid_print(f'removing taxonomy {taxonomy_fields}')
            definition = remove_fields(definition, taxonomy_fields)

        definition.pop('QueryExecutionMode', None) # QueryExecutionMode is supported for export but not for create or update as of 2024-10-17
        definition.pop('QueryExecutionOptions', None) # QueryExecutionOptions is supported for export but not for create or update as of 2024-10-17

        try:
            qs_def = Definition(raw=definition)
            version = str(qs_def.cid_version)
            if version:
                cid_print(f'detected version {version}')
                dashboard_resource['version'] = version
        except Exception as exc:
            logger.warning(f'failed to read version from about tab')

        for dataset in definition.get('DataSetIdentifierDeclarations', []):
            # Hide region and account number of the source account
            dataset["DataSetArn"] = f'arn:{qs.partition}:quicksight:::dataset/' + dataset["DataSetArn"].split('/')[-1]

        if get_parameters().get('one-file'):
            dashboard_resource['data'] = yaml.safe_dump(definition)
        else:
            directory = os.path.dirname(output)
            filename = os.path.basename(output)
            base, ext = os.path.splitext(filename)
            output_definition = f"./{base}-definition{ext}"
            definition_path = os.path.abspath(os.path.join(directory, output_definition))
            with open(definition_path, 'w') as f_:
                f_.write(yaml.safe_dump(definition))
            dashboard_resource['file'] = output_definition

    resources['dashboards'][dashboard_key] = dashboard_resource

    for name, dataset in datasets.items():
        resources['datasets'][name] = dataset

    with open(output, "w", encoding='utf-8') as output_file:
        output_file.write(yaml.safe_dump(resources, sort_keys=False))
    cid_print(f'Output: <BOLD>{output}<END>')


def quick_try():
    ''' just trying the export
    '''
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('cid').setLevel(logging.DEBUG)
    identity = boto3.client('sts').get_caller_identity()
    qs = QuickSight(boto3.session.Session(), identity)
    athena = Athena(boto3.session.Session())
    glue = Glue(boto3.session.Session())
    export_analysis(qs, athena, glue)

if __name__ == "__main__": # for testing
    quick_try()
