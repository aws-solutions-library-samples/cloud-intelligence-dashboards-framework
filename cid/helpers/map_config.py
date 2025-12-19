"""
Account Mapper Configuration Tool

This module provides an interactive configuration interface for the account mapper.
Users can configure Athena settings, file sources, transformation rules, and S3 output
through a menu-driven interface.
"""
import logging
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

from cid.helpers.account_mapper_helpers import (
    ConfigManager,
    get_file_columns,
    get_available_tag_keys,
    _load_sample_accounts_for_preview,
    _show_name_split_preview,
    _select_index_with_preview,
    clear_athena_cache
)

logger = logging.getLogger(__name__)


def run_configuration_menu(athena=None, s3=None, session=None, **kwargs):
    """
    Run interactive configuration menu for account mapper.
    
    This is the main entry point for the map-config command. It provides
    an interactive menu for configuring all aspects of the account mapper.
    
    Args:
        athena: Athena helper instance from cid-cmd (optional)
        s3: S3 helper instance from cid-cmd (optional)
        session: boto3 Session instance from cid-cmd (optional)
        **kwargs: Additional parameters from CLI
    """
    # Get configuration file path from kwargs
    config_file = kwargs.get('config_file', 'config.json')
    
    # Initialize configuration manager
    config_manager = ConfigManager(config_path=config_file)
    
    print("="*60)
    print("⚙️  Account Mapper Configuration")
    print("="*60 + "\n")
    
    # Load existing configuration or create new
    if config_manager.exists():
        try:
            config = config_manager.load()
            print("📂 Loaded existing configuration\n")
        except Exception as e:
            print(f"⚠️  Could not load existing configuration: {e}")
            print("Creating new configuration\n")
            config = {}
    else:
        print("📝 Creating new configuration\n")
        config = {}
    
    # Track if configuration was modified
    modified = False
    
    # Main menu loop
    while True:
        choice = show_main_menu()
        
        if choice == "general":
            config = configure_general_settings(config)
            modified = True
        
        elif choice == "athena":
            config = configure_athena_settings(config, athena, session)
            modified = True
        
        elif choice == "file":
            config = configure_file_source(config)
            modified = True
        
        elif choice == "rules":
            config = configure_rules(config, athena)
            modified = True
        
        elif choice == "s3":
            config = configure_s3_output(config)
            modified = True
        
        elif choice == "summary":
            show_configuration_summary(config)
        
        elif choice == "save":
            # Save and exit
            if not modified:
                print("\n⚠️  No changes to save\n")
                continue
            
            # Validate configuration before saving
            is_valid, errors = validate_config(config)
            
            if not is_valid:
                print("\n⚠️  Configuration is incomplete or invalid:\n")
                for error in errors:
                    print(f"   - {error}")
                print("\n💡 Please complete the configuration before saving\n")
                continue
            
            # Save configuration
            success = config_manager.save(config)
            
            if success:
                print("\n✅ Configuration saved successfully!\n")
                
                # Ask if user wants to execute workflow now
                execute_now = inquirer.confirm(
                    message="Would you like to execute the workflow now?",
                    default=True
                ).execute()
                
                if execute_now:
                    print()
                    # Import and execute the workflow
                    from cid.helpers.account_mapper import execute_workflow
                    execute_workflow(config, athena, s3, session)
                
                break
            else:
                print("\n❌ Failed to save configuration\n")
        
        elif choice == "exit":
            # Exit without saving
            if modified:
                confirm_exit = inquirer.confirm(
                    message="You have unsaved changes. Exit anyway?",
                    default=False
                ).execute()
                
                if not confirm_exit:
                    continue
            
            print("\n👋 Exiting without saving\n")
            break
    
    # Clear Athena data cache when exiting configuration menu
    clear_athena_cache()


def show_main_menu() -> str:
    """
    Display main configuration menu.
    
    Returns:
        User's menu choice
    """
    choices = [
        Choice(value="general", name="⚙️  General Settings"),
        Choice(value="athena", name="🗄️  Athena Configuration"),
        Choice(value="file", name="📁 File Source Configuration"),
        Choice(value="rules", name="📊 Business Rules Configuration"),
        Choice(value="s3", name="☁️  S3 Output Configuration"),
        Separator(),
        Choice(value="summary", name="📋 View Configuration Summary"),
        Choice(value="save", name="💾 Save and Exit"),
        Choice(value="exit", name="🚪 Exit without Saving"),
    ]
    
    result = inquirer.select(
        message="Configuration Menu - Select a section:",
        choices=choices,
        default="general"
    ).execute()
    
    return result


def configure_general_settings(config: dict) -> dict:
    """
    Configure general settings section.
    
    Args:
        config: Current configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    if 'general' not in config:
        config['general'] = {}
    
    print("\n⚙️  General Settings Configuration\n")
    
    # AWS Region
    config['general']['aws_region'] = inquirer.text(
        message="AWS Region:",
        default=config['general'].get('aws_region', 'us-east-1')
    ).execute()
    
    # Log Directory
    config['general']['log_directory'] = inquirer.text(
        message="Log Directory:",
        default=config['general'].get('log_directory', '.logs')
    ).execute()
    
    print("\n✅ General settings updated\n")
    return config


def configure_athena_settings(config: dict, athena=None, session=None) -> dict:
    """
    Configure Athena settings section with dynamic resource loading.

    Loads available catalogs, workgroups, databases, and tables from Athena
    and presents them as searchable selection lists. Auto-selects when only
    one option is available.

    Args:
        config: Current configuration dictionary
        athena: Athena helper instance from cid-cmd (optional)
        session: boto3 Session instance (optional)

    Returns:
        Updated configuration dictionary
    """
    if 'athena' not in config:
        config['athena'] = {}

    print("\n🗄️  Athena Configuration\n")

    # Try to create Athena helper if not provided
    athena_helper = athena
    if not athena_helper and session:
        try:
            from cid.helpers import Athena
            athena_helper = Athena(session=session)
        except Exception as e:
            logger.debug(f"Could not create Athena helper: {e}")
            athena_helper = None

    # --- Catalog Selection ---
    catalog = _select_athena_catalog(config, athena_helper)
    config['athena']['catalog'] = catalog

    # --- Workgroup Selection ---
    workgroup = _select_athena_workgroup(config, athena_helper)
    config['athena']['workgroup'] = workgroup

    # Set workgroup on athena helper for subsequent queries
    if athena_helper and workgroup:
        try:
            athena_helper._WorkGroup = workgroup
        except Exception:
            pass

    # --- Source Database Selection ---
    database = _select_athena_database(config, athena_helper, "Source Database:")
    config['athena']['database'] = database

    # --- Source Table Selection ---
    table = _select_athena_table(config, athena_helper, database, "Source Table:")
    config['athena']['table'] = table

    # --- Target Database Selection ---
    target_db = _select_athena_database(
        config, athena_helper,
        "Target Database (for output view/table):",
        default=database
    )
    config['athena']['database_target'] = target_db

    # --- Output Mode ---
    config['athena']['output_mode'] = inquirer.select(
        message="Output Mode:",
        choices=[
            Choice(value="view", name="Athena View"),
            Choice(value="parquet", name="S3 Parquet with Athena Table")
        ],
        default=config['athena'].get('output_mode', 'view')
    ).execute()

    # --- Output Table Name ---
    config['athena']['output_table_name'] = inquirer.text(
        message="Output Table/View Name:",
        default=config['athena'].get('output_table_name', 'account_map')
    ).execute()

    print("\n✅ Athena settings updated\n")
    return config


def _select_athena_catalog(config: dict, athena_helper) -> str:
    """
    Select Athena catalog from available options.

    Args:
        config: Configuration dictionary
        athena_helper: Athena helper instance (may be None)

    Returns:
        Selected catalog name
    """
    default_catalog = config.get('athena', {}).get('catalog', 'AwsDataCatalog')

    if not athena_helper:
        return inquirer.text(
            message="Athena Catalog:",
            default=default_catalog
        ).execute()

    try:
        print("🔍 Loading available catalogs...")
        catalogs = athena_helper.list_data_catalogs()

        if not catalogs:
            print("⚠️  No catalogs found, using manual input")
            return inquirer.text(
                message="Athena Catalog:",
                default=default_catalog
            ).execute()

        # Auto-select if only one option
        if len(catalogs) == 1:
            print(f"✅ Auto-selected catalog: {catalogs[0]}")
            return catalogs[0]

        # Use fuzzy search for multiple options
        print(f"✅ Found {len(catalogs)} catalogs")
        return inquirer.fuzzy(
            message="Select Athena Catalog:",
            choices=sorted(catalogs, key=lambda x: x != default_catalog),
            default=default_catalog if default_catalog in catalogs else None,
            long_instruction="Type to filter, arrows to navigate"
        ).execute()

    except Exception as e:
        logger.debug(f"Could not list catalogs: {e}")
        print(f"⚠️  Could not load catalogs: {e}")
        return inquirer.text(
            message="Athena Catalog:",
            default=default_catalog
        ).execute()


def _select_athena_workgroup(config: dict, athena_helper) -> str:
    """
    Select Athena workgroup from available options.

    Args:
        config: Configuration dictionary
        athena_helper: Athena helper instance (may be None)

    Returns:
        Selected workgroup name
    """
    default_workgroup = config.get('athena', {}).get('workgroup', 'primary')

    if not athena_helper:
        return inquirer.text(
            message="Athena Workgroup:",
            default=default_workgroup
        ).execute()

    try:
        print("🔍 Loading available workgroups...")
        workgroups_raw = athena_helper.list_work_groups()

        if not workgroups_raw:
            print("⚠️  No workgroups found, using manual input")
            return inquirer.text(
                message="Athena Workgroup:",
                default=default_workgroup
            ).execute()

        # Extract workgroup names
        workgroups = [wg['Name'] for wg in workgroups_raw if wg.get('Name')]

        if not workgroups:
            return inquirer.text(
                message="Athena Workgroup:",
                default=default_workgroup
            ).execute()

        # Auto-select if only one option
        if len(workgroups) == 1:
            print(f"✅ Auto-selected workgroup: {workgroups[0]}")
            return workgroups[0]

        # Use fuzzy search for multiple options
        print(f"✅ Found {len(workgroups)} workgroups")
        return inquirer.fuzzy(
            message="Select Athena Workgroup:",
            choices=sorted(workgroups, key=lambda x: x != default_workgroup),
            default=default_workgroup if default_workgroup in workgroups else None,
            long_instruction="Type to filter, arrows to navigate"
        ).execute()

    except Exception as e:
        logger.debug(f"Could not list workgroups: {e}")
        print(f"⚠️  Could not load workgroups: {e}")
        return inquirer.text(
            message="Athena Workgroup:",
            default=default_workgroup
        ).execute()


def _select_athena_database(config: dict, athena_helper, message: str, default: str = None) -> str:
    """
    Select Athena database from available options.

    Args:
        config: Configuration dictionary
        athena_helper: Athena helper instance (may be None)
        message: Prompt message to display
        default: Default value to use

    Returns:
        Selected database name
    """
    default_db = default or config.get('athena', {}).get('database', '')

    if not athena_helper:
        return inquirer.text(
            message=message,
            default=default_db
        ).execute()

    try:
        catalog = config.get('athena', {}).get('catalog', 'AwsDataCatalog')
        print(f"🔍 Loading databases from catalog '{catalog}'...")
        databases = athena_helper.list_databases(catalog_name=catalog)

        if not databases:
            print("⚠️  No databases found, using manual input")
            return inquirer.text(
                message=message,
                default=default_db
            ).execute()

        # Auto-select if only one option
        if len(databases) == 1:
            print(f"✅ Auto-selected database: {databases[0]}")
            return databases[0]

        # Use fuzzy search for multiple options
        print(f"✅ Found {len(databases)} databases")
        return inquirer.fuzzy(
            message=message,
            choices=sorted(databases, key=lambda x: x != default_db),
            default=default_db if default_db in databases else None,
            long_instruction="Type to filter, arrows to navigate"
        ).execute()

    except Exception as e:
        logger.debug(f"Could not list databases: {e}")
        print(f"⚠️  Could not load databases: {e}")
        return inquirer.text(
            message=message,
            default=default_db
        ).execute()


def _select_athena_table(config: dict, athena_helper, database: str, message: str) -> str:
    """
    Select Athena table from available options in the specified database.

    Args:
        config: Configuration dictionary
        athena_helper: Athena helper instance (may be None)
        database: Database name to list tables from
        message: Prompt message to display

    Returns:
        Selected table name
    """
    default_table = config.get('athena', {}).get('table', '')

    if not athena_helper or not database:
        return inquirer.text(
            message=message,
            default=default_table
        ).execute()

    try:
        print(f"🔍 Loading tables from database '{database}'...")
        tables_metadata = athena_helper.list_table_metadata(database_name=database)

        if not tables_metadata:
            print("⚠️  No tables found, using manual input")
            return inquirer.text(
                message=message,
                default=default_table
            ).execute()

        # Extract table names
        tables = [t['Name'] for t in tables_metadata if t.get('Name')]

        if not tables:
            return inquirer.text(
                message=message,
                default=default_table
            ).execute()

        # Auto-select if only one option
        if len(tables) == 1:
            print(f"✅ Auto-selected table: {tables[0]}")
            return tables[0]

        # Use fuzzy search for multiple options
        print(f"✅ Found {len(tables)} tables")
        return inquirer.fuzzy(
            message=message,
            choices=sorted(tables, key=lambda x: x != default_table),
            default=default_table if default_table in tables else None,
            long_instruction="Type to filter, arrows to navigate"
        ).execute()

    except Exception as e:
        logger.debug(f"Could not list tables: {e}")
        print(f"⚠️  Could not load tables: {e}")
        return inquirer.text(
            message=message,
            default=default_table
        ).execute()


def configure_file_source(config: dict) -> dict:
    """
    Configure file source section.
    
    Args:
        config: Current configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    if 'file_source' not in config:
        config['file_source'] = {}
    
    print("\n📁 File Source Configuration\n")
    
    # Enable file source
    config['file_source']['enabled'] = inquirer.confirm(
        message="Enable file source for additional data?",
        default=config['file_source'].get('enabled', False)
    ).execute()
    
    if config['file_source']['enabled']:
        # File path with file picker
        default_path = config['file_source'].get('file_path', '')
        
        config['file_source']['file_path'] = inquirer.filepath(
            message="Select File (Excel, CSV, or JSON):",
            default=default_path if default_path else str(Path.cwd()),
            validate=lambda path: Path(path).exists() or "File does not exist",
            only_files=True
        ).execute()
        
        # Get columns from the selected file
        file_columns = get_file_columns(config['file_source']['file_path'])
        
        if file_columns:
            print(f"\n✅ Found {len(file_columns)} columns in file\n")
            
            # Account ID column - select from available columns
            config['file_source']['account_id_column'] = inquirer.select(
                message="Select Account ID Column:",
                choices=file_columns,
                default=config['file_source'].get('account_id_column', 
                         'account_id' if 'account_id' in file_columns else file_columns[0])
            ).execute()
            
            # Account Name column - select from available columns
            config['file_source']['account_name_column'] = inquirer.select(
                message="Select Account Name Column:",
                choices=file_columns,
                default=config['file_source'].get('account_name_column',
                         'account_name' if 'account_name' in file_columns else file_columns[0])
            ).execute()
            
            # Payer Name column (optional) - select from available columns or skip
            payer_choices = ['(Skip - No Payer Column)'] + file_columns
            payer_default = config['file_source'].get('payer_name_column', '')
            
            payer_selection = inquirer.select(
                message="Select Payer Name Column (optional):",
                choices=payer_choices,
                default=payer_default if payer_default in file_columns else payer_choices[0]
            ).execute()
            
            config['file_source']['payer_name_column'] = '' if payer_selection == payer_choices[0] else payer_selection
        else:
            # Fallback to text input if columns cannot be read
            print("⚠️  Could not read file columns, using manual input\n")
            
            config['file_source']['account_id_column'] = inquirer.text(
                message="Account ID Column Name:",
                default=config['file_source'].get('account_id_column', 'account_id')
            ).execute()
            
            config['file_source']['account_name_column'] = inquirer.text(
                message="Account Name Column Name:",
                default=config['file_source'].get('account_name_column', 'account_name')
            ).execute()
            
            config['file_source']['payer_name_column'] = inquirer.text(
                message="Payer Name Column Name (optional, press Enter to skip):",
                default=config['file_source'].get('payer_name_column', '')
            ).execute()
    
    print("\n✅ File source settings updated\n")
    return config


def configure_rules(config: dict, athena=None) -> dict:
    """
    Configure business rules section.
    
    Args:
        config: Current configuration dictionary
        athena: Optional Athena helper instance from cid-cmd
        
    Returns:
        Updated configuration dictionary
    """
    if 'rules' not in config:
        config['rules'] = {'hierarchy_levels': []}
    
    if 'hierarchy_levels' not in config['rules']:
        config['rules']['hierarchy_levels'] = []
    
    while True:
        print("\n📊 Business Rules Configuration\n")
        
        # Display current hierarchy levels
        if config['rules']['hierarchy_levels']:
            print("Current Hierarchy Levels:")
            for level in config['rules']['hierarchy_levels']:
                print(f"  Level {level['level']}: {level['name']} (source: {level['source']})")
            print()
        else:
            print("No hierarchy levels configured yet.\n")
        
        # Menu choices
        choices = [
            Choice(value="add", name="➕ Add Hierarchy Level"),
        ]
        
        if config['rules']['hierarchy_levels']:
            choices.extend([
                Choice(value="modify", name="✏️  Modify Hierarchy Level"),
                Choice(value="remove", name="❌ Remove Hierarchy Level"),
            ])
        
        choices.append(Choice(value="done", name="✅ Done"))
        
        action = inquirer.select(
            message="Select an action:",
            choices=choices
        ).execute()
        
        if action == "add":
            config = add_hierarchy_level(config, athena)
        elif action == "modify":
            config = modify_hierarchy_level(config, athena)
        elif action == "remove":
            config = remove_hierarchy_level(config)
        elif action == "done":
            break
    
    print("\n✅ Business rules updated\n")
    return config


def add_hierarchy_level(config: dict, athena=None) -> dict:
    """
    Add a new hierarchy level.
    
    Args:
        config: Current configuration dictionary
        athena: Optional Athena helper instance from cid-cmd
        
    Returns:
        Updated configuration dictionary
    """
    print("\n➕ Add Hierarchy Level\n")
    
    # Automatically determine next level number
    existing_levels = [level['level'] for level in config['rules']['hierarchy_levels']]
    level_num = max(existing_levels) + 1 if existing_levels else 1
    
    print(f"📊 Creating Level {level_num}\n")
    
    # Level name
    level_name = inquirer.text(
        message="Level Name (e.g., 'Business Unit', 'Department'):",
        default=f"Level {level_num}"
    ).execute()
    
    # Source type
    source = inquirer.select(
        message="Data Source:",
        choices=[
            Choice(value="athena_tags", name="Athena Tags (hierarchytags)"),
            Choice(value="athena_name", name="Account Name (split by separator)"),
            Choice(value="athena_payer", name="Payer Account Information"),
            Choice(value="file", name="External File")
        ]
    ).execute()
    
    # Get parameters based on source type
    parameters = {}
    
    if source == "athena_tags":
        # Try to get available tag keys from Athena
        tag_keys = get_available_tag_keys(config, athena)
        
        if tag_keys:
            print(f"\n✅ Found {len(tag_keys)} tag keys in Athena data\n")
            parameters['tag_key'] = inquirer.select(
                message="Select Tag Key:",
                choices=tag_keys,
                default=tag_keys[0]
            ).execute()
        else:
            print("\n⚠️  Could not retrieve tag keys from Athena, using manual input\n")
            parameters['tag_key'] = inquirer.text(
                message="Tag Key:",
                default=""
            ).execute()
    
    elif source == "athena_name":
        # Load sample accounts for preview
        sample_accounts = _load_sample_accounts_for_preview(config, athena)
        
        parameters['separator'] = inquirer.text(
            message="Separator Character:",
            default="-"
        ).execute()
        
        # Show preview with the selected separator
        if sample_accounts:
            print("\n💡 Showing live preview with your actual account data:")
            _show_name_split_preview(sample_accounts, parameters['separator'])
            parameters['index'] = _select_index_with_preview(sample_accounts, parameters['separator'], level_name)
        else:
            print("\n⚠️  Preview not available - using manual input")
            parameters['index'] = int(inquirer.number(
                message="Index (0-based):",
                default=0,
                min_allowed=0
            ).execute())
    
    elif source == "file":
        # Try to get columns from configured file
        file_path = config.get('file_source', {}).get('file_path')
        file_columns = get_file_columns(file_path) if file_path else None
        
        if file_columns:
            print(f"\n✅ Found {len(file_columns)} columns in file\n")
            
            parameters['column_name'] = inquirer.select(
                message="Select Column for Hierarchy Level:",
                choices=file_columns,
                default=file_columns[0]
            ).execute()
            
            parameters['account_id_column'] = inquirer.select(
                message="Select Account ID Column:",
                choices=file_columns,
                default='account_id' if 'account_id' in file_columns else file_columns[0]
            ).execute()
        else:
            if file_path:
                print("\n⚠️  Could not read file columns, using manual input\n")
            else:
                print("\n⚠️  No file configured yet, using manual input\n")
            
            parameters['column_name'] = inquirer.text(
                message="Column Name in File:",
                default=""
            ).execute()
            
            parameters['account_id_column'] = inquirer.text(
                message="Account ID Column in File:",
                default="account_id"
            ).execute()
    
    elif source == "athena_payer":
        # For payer source, we can add custom payer name mapping later if needed
        parameters['info_type'] = 'name'  # Default to payer name
    
    # Create new level
    new_level = {
        'level': int(level_num),
        'name': level_name,
        'source': source,
        'parameters': parameters
    }
    
    config['rules']['hierarchy_levels'].append(new_level)
    
    # Sort by level number
    config['rules']['hierarchy_levels'].sort(key=lambda x: x['level'])
    
    print(f"\n✅ Added level {level_num}: {level_name}\n")
    return config


def modify_hierarchy_level(config: dict, athena=None) -> dict:
    """
    Modify an existing hierarchy level.
    
    Args:
        config: Current configuration dictionary
        athena: Optional Athena helper instance from cid-cmd
        
    Returns:
        Updated configuration dictionary
    """
    if not config['rules']['hierarchy_levels']:
        print("\n⚠️  No hierarchy levels to modify\n")
        return config
    
    print("\n✏️  Modify Hierarchy Level\n")
    
    # Select level to modify
    choices = [
        Choice(
            value=idx,
            name=f"Level {level['level']}: {level['name']} (source: {level['source']})"
        )
        for idx, level in enumerate(config['rules']['hierarchy_levels'])
    ]
    
    idx = inquirer.select(
        message="Select level to modify:",
        choices=choices
    ).execute()
    
    # Remove the old level and add it back with modifications
    old_level = config['rules']['hierarchy_levels'].pop(idx)
    
    print(f"\nModifying: Level {old_level['level']}: {old_level['name']}\n")
    
    # For simplicity, just remove and re-add
    # User can configure it fresh
    print("Please reconfigure this level:\n")
    config = add_hierarchy_level(config, athena)
    
    return config


def remove_hierarchy_level(config: dict) -> dict:
    """
    Remove a hierarchy level.
    
    Args:
        config: Current configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    if not config['rules']['hierarchy_levels']:
        print("\n⚠️  No hierarchy levels to remove\n")
        return config
    
    print("\n❌ Remove Hierarchy Level\n")
    
    # Select level to remove
    choices = [
        Choice(
            value=idx,
            name=f"Level {level['level']}: {level['name']} (source: {level['source']})"
        )
        for idx, level in enumerate(config['rules']['hierarchy_levels'])
    ]
    
    idx = inquirer.select(
        message="Select level to remove:",
        choices=choices
    ).execute()
    
    removed_level = config['rules']['hierarchy_levels'].pop(idx)
    print(f"\n✅ Removed level {removed_level['level']}: {removed_level['name']}\n")
    
    return config


def configure_s3_output(config: dict) -> dict:
    """
    Configure S3 output section.
    
    Args:
        config: Current configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    if 's3_output' not in config:
        config['s3_output'] = {}
    
    print("\n☁️  S3 Output Configuration\n")
    print("(Only needed if output mode is 'parquet')\n")
    
    # S3 Bucket
    config['s3_output']['bucket'] = inquirer.text(
        message="S3 Bucket Name:",
        default=config['s3_output'].get('bucket', '')
    ).execute()
    
    # S3 Prefix
    config['s3_output']['prefix'] = inquirer.text(
        message="S3 Prefix/Path:",
        default=config['s3_output'].get('prefix', 'account-map')
    ).execute()
    
    # Table Name
    config['s3_output']['table_name'] = inquirer.text(
        message="Athena Table Name:",
        default=config['s3_output'].get('table_name', 'account_map')
    ).execute()
    
    print("\n✅ S3 output settings updated\n")
    return config


def show_configuration_summary(config: dict):
    """
    Display current configuration in readable format.
    
    Args:
        config: Configuration dictionary to display
    """
    print("\n" + "="*60)
    print("📋 Configuration Summary")
    print("="*60 + "\n")
    
    # General Settings
    if 'general' in config:
        print("⚙️  General Settings:")
        print(f"   AWS Region: {config['general'].get('aws_region', 'Not set')}")
        print(f"   Log Directory: {config['general'].get('log_directory', 'Not set')}")
        print()
    
    # Athena Settings
    if 'athena' in config:
        print("🗄️  Athena Configuration:")
        print(f"   Catalog: {config['athena'].get('catalog', 'Not set')}")
        print(f"   Workgroup: {config['athena'].get('workgroup', 'Not set')}")
        print(f"   Database: {config['athena'].get('database', 'Not set')}")
        print(f"   Table: {config['athena'].get('table', 'Not set')}")
        print(f"   Target Database: {config['athena'].get('database_target', 'Not set')}")
        print(f"   Output Mode: {config['athena'].get('output_mode', 'Not set')}")
        print(f"   Output Table/View: {config['athena'].get('output_table_name', 'Not set')}")
        print()
    
    # File Source
    if 'file_source' in config:
        print("📁 File Source:")
        enabled = config['file_source'].get('enabled', False)
        print(f"   Enabled: {enabled}")
        if enabled:
            print(f"   File Path: {config['file_source'].get('file_path', 'Not set')}")
            print(f"   Account ID Column: {config['file_source'].get('account_id_column', 'Not set')}")
            print(f"   Account Name Column: {config['file_source'].get('account_name_column', 'Not set')}")
        print()
    
    # Business Rules
    if 'rules' in config and config['rules'].get('hierarchy_levels'):
        print("📊 Business Rules:")
        print(f"   Hierarchy Levels: {len(config['rules']['hierarchy_levels'])}")
        for level in config['rules']['hierarchy_levels']:
            print(f"      Level {level['level']}: {level['name']} (source: {level['source']})")
        print()
    
    # S3 Output
    if 's3_output' in config:
        print("☁️  S3 Output:")
        print(f"   Bucket: {config['s3_output'].get('bucket', 'Not set')}")
        print(f"   Prefix: {config['s3_output'].get('prefix', 'Not set')}")
        print(f"   Table Name: {config['s3_output'].get('table_name', 'Not set')}")
        print()
    
    print("="*60 + "\n")


def validate_config(config: dict) -> Tuple[bool, List[str]]:
    """
    Validate complete configuration.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Validate general settings
    if 'general' not in config:
        errors.append("General settings section is missing")
    else:
        if not config['general'].get('aws_region'):
            errors.append("AWS region is required in general settings")
    
    # Validate Athena settings
    if 'athena' not in config:
        errors.append("Athena settings section is missing")
    else:
        if not config['athena'].get('database'):
            errors.append("Athena database is required")
        if not config['athena'].get('table'):
            errors.append("Athena table is required")
        if not config['athena'].get('output_mode'):
            errors.append("Output mode is required")
        if not config['athena'].get('output_table_name'):
            errors.append("Output table/view name is required")
    
    # Validate S3 output if mode is parquet
    if config.get('athena', {}).get('output_mode') == 'parquet':
        if 's3_output' not in config:
            errors.append("S3 output settings required for parquet mode")
        elif not config['s3_output'].get('bucket'):
            errors.append("S3 bucket is required for parquet mode")
    
    # Validate rules
    if 'rules' not in config or not config['rules'].get('hierarchy_levels'):
        errors.append("At least one hierarchy level is required")
    
    is_valid = len(errors) == 0
    return is_valid, errors
