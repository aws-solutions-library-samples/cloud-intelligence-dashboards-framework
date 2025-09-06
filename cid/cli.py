import os
import logging
import platform

import click

from cid.common import Cid
from cid.utils import get_parameters, set_parameters, get_latest_tool_version, cid_print
from cid._version import __version__
from cid.exceptions import CidCritical, CidError

logger = logging.getLogger('cid')
version = __version__
latest_version = get_latest_tool_version()
prog_name="CLOUD INTELLIGENCE DASHBOARDS (CID) CLI"
print(f'{prog_name} {version}\n')

if __version__ != latest_version and latest_version != 'UNDEFINED':
    cid_print(f'<YELLOW>A new version {latest_version} is available, please update <BLUE><BOLD>pip install -U cid-cmd<END>\n\n')

def cid_command(func):
    def wrapper(ctx, **kwargs):

        def get_command_line():
            params = get_parameters()
            return ('cid-cmd ' + ctx.info_name
                + ''.join([f" --{k.replace('_','-')}" for k, v in ctx.params.items() if isinstance(v, bool) and v])
                + ''.join([f" --{k.replace('_','-')} '{v}'" for k, v in ctx.params.items() if not isinstance(v, bool) and v is not None])
                + ''.join([f" --{k} '{v}' " for k, v in params.items() if not isinstance(v, bool) and v is not None])
            )

        # Complete kwargs with other parameters
        if len(ctx.args) % 2 != 0:
            print(f"Unknown extra argument, or an option without value {ctx.args}")
            exit(-1)
        params = {}
        for i in range(0, len(ctx.args), 2):
            key = ctx.args[i][2:].replace('-', '_')
            params[key] = ctx.args[i+1]
        set_parameters(params, all_yes=ctx.obj.all_yes)

        res = None
        try:
            res = func(ctx, **kwargs)
        except (CidCritical, CidError) as exc:
            logger.debug(exc, exc_info=True)
            logger.debug(f'When running {get_command_line()}')
            logger.error(exc)
        params = get_parameters()
        logger.info('Next time you can use following command:')
        logger.info(get_command_line())
        return res
    wrapper.__doc__ = func.__doc__
    wrapper.__name__ = func.__name__
    return main.command(
        context_settings=dict(
            ignore_unknown_options=True,
            allow_extra_args=True,
        )
    )(click.pass_context(wrapper))


@click.group()
@click.option('--profile_name', '--profile', help='AWS Profile name to use', default=None)
@click.option('--region_name', help="AWS Region", default=None)
@click.option('--aws_access_key_id', help='', default=None)
@click.option('--aws_secret_access_key', help='', default=None)
@click.option('--aws_session_token', help='', default=None)
@click.option('--log_filename', help='log file name', default='cid.log')
@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@click.pass_context
def main(ctx, **kwargs):

    # enable color for windows terminal
    if platform.system() == "Windows":
        os.system('color') #nosec B605, B607

    ctx.obj = Cid(**kwargs)

@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def map(ctx, **kwargs):
    """Create account mapping

    \b
    Command options:
     --cur-table-name TEXT                 CUR table name
     --athena-database TEXT                Athena database
     --glue-data-catalog TEXT              Glue data catalog
     --account-map-source TEXT             csv, dummy, organization (if autodiscovery impossible)
     --account-map-file TEXT               csv file path relative to current directory (if autodiscovery impossible and csv selected as a source )
    """
    ctx.obj.map(**kwargs)

@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def csv2view(ctx, **kwargs):
    """Create account sql code from CSV file

    \b
    Command options:
     --input                         csv file
     --name                          Athena View name
    """
    ctx.obj.csv2view(**kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@click.option('--share-with-account', help='Share dashboard with all users in the current account', is_flag=True, default=None)
@click.option('--quicksight-delete-failed-datasource', help='Delete datasoruce if creation failed', is_flag=True, default=None)
@cid_command
def deploy(ctx, **kwargs):
    """Deploy Dashboard

    \b
    Command options:
     --category TEXT                       The dashboards category to choose from. Not needed if dashboard-id provided directly
     --dashboard-id TEXT                   QuickSight dashboard id (cudos, cost_intelligence_dashboard, kpi_dashboard, ta-organizational-view, trends-dashboard etc)
     --athena-database TEXT                Athena database
     --athena-workgroup TEXT               Athena workgroup
     --glue-data-catalog TEXT              Glue data catalog
     --cur-table-name TEXT                 CUR table name
     --quicksight-datasource-id TEXT       QuickSight Datasource ARN (if not found one with provided Athena workgroup)
     --quicksight-datasource-role-arn TEXT IAM Role used for DataSource Creation (if not provided, will use the default QS Role). Must have access to Athena and S3 buckets.
     --allow-buckets                       Comma separated list of buckets names to add to the default Cid QuickSight role
     --quicksight-user TEXT                QuickSight user
     --quicksight-group TEXT               QuickSight group
     --dataset-{dataset_name}-id TEXT      QuickSight dataset id for a specific dataset
     --view-{view_name}-{parameter} TEXT   a custom parameter for a view creation, can use variable: {account_id}
     --account-map-source TEXT             csv, dummy, organization (if autodiscovery impossible)
     --account-map-file TEXT               csv file path relative to current directory (if autodiscovery impossible and csv selected as a source )
     --on-drift (show|override)            Action if a drift of view and dataset is discovered. 'override' = override drift(will destroy customization) or 'show' (default) = show a diff. In Unattended mode (without terminal on-drift will have allways override behaviour)
     --update (yes|no)                     Update if some elements are already installed. Default = 'no'
     --resources TEXT                      CID resources yaml file or url
     --category TEXT                       Comma separated list of categories of dashboards (ex: foundational,advanced )
     --catalog TEXT                        Comma separated list of catalog files or urls (ex: foundational,advanced )
     --theme TEXT                          A QuickSight Theme (CLASSIC|MIDNIGHT|SEASIDE|RAINIER)
     --currency TEXT                       A currency symbol instead of default USD (USD|GBP|EUR|JPY|KRW|DKK|TWD|INR)
     --rls TEXT                            Desired RLS status (CLEAR|ENABLED|DISABLED) Clear = No RLS configuration.
     --rls-dataset-id TEXT                 ID of RLS Dataset that will be used
     --share-with-account  (yes|no)        Make dashboard visible to other users in the same account by default.
     """
    ctx.obj.deploy(**kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def export(ctx, **kwargs):
    """Export Dashboard

    \b
    Command options:
        --analysis-name              Analysis you want to share (not needed if analysis-id is provided).
        --analysis-id                ID of analysis you want to share (open analysis in browser and copy id from url)
        --one-file (no|yes)          Default=no, if set export generates a single file if omitted (default) the dashboard will be in a separate file     
        --template-id                Template Id
        --dashboard-id               Target Dashboard Id
        --template-version           Version description vX.Y.Z
        --taxonomy                   list of fields that export will keep as global filters. Only if these global filters exist.
        --reader-account             Account id with whom you want to share with or *
        --dashboard-export-method
               (definition|template) A method (definition=pull json definition of Analysis OR template=create QuickSight Template)
        --export-known-datasets
            (no|yes)                 If 'yes' the export will include DataSets that are already in resources file. Default = no
        --export-tables (no|yes).    If 'yes' the export will include Tables. Default: no = only Views
        --category TEXT              The dashboards category. Default = Custom
        --output                     A filename (.yaml) If provided an existing file it will be analyzed for default values and overridden 
    """
    ctx.obj.export(**kwargs)


@click.option('--dashboard-id', help='QuickSight dashboard id', default=None)
@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def status(ctx, dashboard_id, **kwargs):
    """Show Dashboard status"""
    ctx.obj.status(dashboard_id, **kwargs)


@click.option('--dashboard-id', help='QuickSight dashboard id', default=None)
@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def delete(ctx, dashboard_id, **kwargs):
    """Delete Dashboard and all dependencies unused by other CID-managed dashboards
    (including QuickSight datasets, Athena views and tables)

    \b
    Command options:
     --dashboard-id TEXT                   QuickSight dashboard id (cudos, cost_intelligence_dashboard, kpi_dashboard, ta-organizational-view, trends-dashboard etc)
     --athena-database TEXT                Athena database
     """
    ctx.obj.delete(dashboard_id, **kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@click.option('--dashboard-id', help='QuickSight dashboard id', default=None)
@click.option('--force/--noforce', help='allow selecting  up to date dashboards (flags must be before options)', default=False)
@click.option('--recursive/--norecursive', help='Recursive update all Datasets and Views (flags must be before options)', default=False)
@cid_command
def update(ctx, dashboard_id, force, recursive, **kwargs):
    """Update Dashboard

    \b
     --on-drift (show|override)            Action if a drift of view and dataset is discovered. 'override' = override drift(will destroy customization) or 'show' (default) = show a diff. In Unattended mode (without terminal on-drift will have allways override behaviour)
     --theme TEXT                          A QuickSight Theme (CLASSIC|MIDNIGHT|SEASIDE|RAINIER)
     --currency TEXT                       A currency symbol instead of default USD (USD|GBP|EUR|JPY|KRW|DKK|TWD|INR)
     --rls TEXT                            Desired RLS status (CLEAR|ENABLED|DISABLED) Clear = No RLS configuration.
     --rls-dataset-id TEXT                 ID of RLS Dataset that will be used

    """
    ctx.obj.update(dashboard_id, force=force, recursive=recursive, **kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@click.option('--dashboard-id', help='QuickSight dashboard id', default=None)
@cid_command
def open(ctx, dashboard_id, **kwargs):
    """Open Dashboard in browser"""
    ctx.obj.open(dashboard_id, **kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def cleanup(ctx, **kwargs):
    """Delete unused resources (QuickSight datasets, Athena views)"""
    ctx.obj.cleanup(**kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@click.option('--dashboard-id', help='QuickSight dashboard id', default=None)
@click.option('--share-method', help='Sharing method', default=None, type=click.Choice(['folder', 'user', 'account']))
@click.option('--folder-method', help='Folder to use', default=None, type=click.Choice(['new', 'existing']))
@click.option('--folder-id', help='QuickSight folder id (existing)', default=None)
@click.option('--folder-name', help='QuickSight folder name (new)', default=None)
@click.option('--quicksight-user', help='QuickSight user', default=None)
@click.option('--quicksight-group', help='QuickSight group', default=None)
@cid_command
def share(ctx, dashboard_id, **kwargs):
    """Share QuickSight resources (Dashboard, Datasets, DataSource)"""
    ctx.obj.share(dashboard_id)

@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def init_qs(ctx, **kwargs):
    """Initialize Amazon QuickSight

    \b
     --enable-quicksight-enterprise (yes|no) Confirm the activation of QuickSight
     --account-name NAME                     Unique QuickSight account name (Unique across all AWS users)
     --notification-email EMAIL              User's email for QuickSight notifications
    """

    ctx.obj.init_qs(**kwargs)

@click.option('-v', '--verbose', count=True)
@cid_command
def create_cur_table(ctx, **kwargs):
    """Initialize CUR table

    \b
     --view-cur-location  s3://BUCKET/PATH   S3 path with CUR data. We support only 2 types of CUR path: 's3://{bucket}/cur' and 's3://{bucket}/{prefix}/{cur_name}/{cur_name}'
     --crawler-role       ROLE               Name or ARN of crawler role
    """

    ctx.obj.create_cur_table(**kwargs)

@click.option('-v', '--verbose', count=True)
@click.option('--cur-version', help='Cur Version (1 or 2)')
@click.option('--fields', help='CUR fields', default='')
@cid_command
def create_cur_proxy(ctx, cur_version, fields, **kwargs):
    """Create CUR proxy - an Athena view that transforms cur1 to cur2 or cur2 > cur1

    \b
     --cur-version  (1|2)        The target version of CUR
     --fields                    Comma Separated list of additional CUR fields
     --cur-table-name TEXT       CUR table name
     --cur-database TEXT         Athena database of CUR
     --athena-database TEXT      Athena database to create proxy
    """

    ctx.obj.create_cur_proxy(cur_version=cur_version, fields=fields, **kwargs)


@click.option('-v', '--verbose', count=True)
@click.option('-y', '--yes', help='confirm all', is_flag=True, default=False)
@cid_command
def teardown(ctx, **kwargs):
    """Delete all CID assets

    \b
    THIS IS VERY DANGEROUS. DO NOT USE THIS COMMAND.
    """

    ctx.obj.teardown(**kwargs)

if __name__ == '__main__':
    main()


