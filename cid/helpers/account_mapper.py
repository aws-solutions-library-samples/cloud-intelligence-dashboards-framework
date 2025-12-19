"""
Account Mapper - Transform AWS organization data into structured account maps.

This module provides functionality to load organization data from Athena,
apply transformation rules, and output account mapping views.
"""
import logging
import sys

from cid.helpers.account_mapper_helpers import (
    ConfigManager,
    DataLoader,
    TransformEngine
)

logger = logging.getLogger(__name__)


def account_mapper(athena=None, s3=None, session=None, **kwargs):
    """
    Main account mapper workflow.
    
    This function orchestrates the account mapping process:
    1. Load configuration from file
    2. Load organization data from Athena
    3. Apply transformation rules
    4. Write output to Athena view or S3
    
    Args:
        athena: Athena helper instance from cid-cmd
        s3: S3 helper instance from cid-cmd
        session: boto3 Session instance from cid-cmd
        **kwargs: Additional parameters from CLI
    """
    logger.info("Starting Account Mapper Workflow")
    
    # Get configuration file path from kwargs
    config_file = kwargs.get('config_file', 'config.json')
    
    # Initialize configuration manager
    config_manager = ConfigManager(config_path=config_file)
    
    # Check if configuration exists and is valid
    if not config_manager.exists():
        print("\n⚠️  No configuration file found")
        print(f"Please create a configuration file at: {config_file}")
        print("Or run 'cid-cmd map-config' to create one interactively.\n")
        sys.exit(1)
    
    if not config_manager.is_valid():
        print("\n⚠️  Configuration is incomplete or invalid\n")
        missing = config_manager.get_missing_fields()
        if missing:
            print("Missing or invalid fields:")
            for field in missing:
                print(f"  - {field}")
        print("\nPlease fix the configuration file or run 'cid-cmd map-config' to reconfigure.\n")
        sys.exit(1)
    
    # Configuration is valid - execute workflow
    print("\n✅ Valid configuration found\n")
    config = config_manager.load()
    execute_workflow(config, athena, s3, session)

def execute_workflow(config: dict, athena=None, s3=None, session=None):
    """
    Execute the main account mapping workflow.
    
    Args:
        config: Configuration dictionary
        athena: Athena helper instance (optional, will create if not provided)
        s3: S3 helper instance (optional, will create if not provided)
        session: boto3 Session instance (optional, will create if not provided)
    """
    try:
        # Initialize helpers if not provided
        if not athena:
            import boto3
            from cid.helpers import Athena
            region = config.get('general', {}).get('aws_region', 'us-east-1')
            session = session or boto3.Session(region_name=region)
            athena = Athena(session=session)

        # Set Athena settings from config to avoid interactive prompts
        athena_config = config.get('athena', {})
        if athena_config.get('workgroup'):
            athena._WorkGroup = athena_config['workgroup']
        if athena_config.get('catalog'):
            athena._CatalogName = athena_config['catalog']

        if not s3:
            from cid.helpers import S3
            s3 = S3(session=athena.session)

        # Step 1: Load organization data from Athena
        print("📥 Loading organization data from Athena...")
        logger.info("Loading organization data from Athena")
        data_loader = DataLoader(athena, config)
        org_data = data_loader.load_from_athena()
        
        if org_data.empty:
            print("❌ No organization data found")
            logger.error("No organization data returned from Athena")
            return
        
        print(f"✅ Loaded {len(org_data)} accounts from Athena\n")
        logger.info("Successfully loaded %d accounts", len(org_data))
        
        # Step 2: Load file data if configured
        file_data = None
        if config.get('file_source', {}).get('enabled', False):
            print("📁 Loading additional data from file...")
            logger.info("Loading file data")
            
            try:
                file_data = data_loader.load_from_file()
                print(f"✅ Loaded {len(file_data)} records from file\n")
                logger.info("Successfully loaded %d records from file", len(file_data))
            except Exception as e:
                print(f"⚠️  Warning: Could not load file data: {e}\n")
                logger.warning("Failed to load file data: %s", str(e))
        
        # Step 3: Transform data
        print("🔄 Transforming data according to configured rules...")
        logger.info("Starting data transformation")
        
        transform_engine = TransformEngine(config, org_data, file_data)
        account_map = transform_engine.transform()
        
        if account_map.empty:
            print("❌ Transformation produced no results")
            logger.error("Transformation produced empty DataFrame")
            return
        
        print(f"✅ Transformed {len(account_map)} accounts\n")
        logger.info("Transformation complete: %d accounts", len(account_map))
        
        # Step 4: Write output based on output_mode
        output_mode = config['athena'].get('output_mode', 'view')
        database_target = config['athena'].get('database_target', config['athena']['database'])
        output_table_name = config['athena'].get('output_table_name', 'account_map')
        
        if output_mode == 'view':
            # Write to Athena views using AthenaWriter (handles SQL size limits and splitting)
            print("📊 Writing results to Athena view...")
            logger.info("Writing to Athena view: %s.%s", database_target, output_table_name)
            
            try:
                from cid.helpers.account_mapper_helpers import AthenaWriter
                
                # Use AthenaWriter with existing Athena helper
                # AthenaWriter handles SQL size limits, automatic splitting, and UNION view creation
                writer = AthenaWriter(config, athena_helper=athena)
                success = writer.write_as_view(account_map, output_table_name)
                
                if success:
                    print(f"✅ Successfully created view: {database_target}.{output_table_name}\n")
                    logger.info("Successfully created Athena view")
                else:
                    print(f"❌ Failed to create view: {database_target}.{output_table_name}\n")
                    logger.error("Failed to create Athena view")
                    return
                    
            except Exception as e:
                print(f"❌ Failed to create view: {database_target}.{output_table_name}")
                print(f"Error: {e}\n")
                logger.error("Failed to create Athena view: %s", str(e), exc_info=True)
                return
        
        elif output_mode == 'parquet':
            # Write to S3 Parquet and create Athena table
            print("☁️  Writing results to S3 as Parquet...")
            logger.info("Writing to S3 Parquet")
            
            from cid.helpers.account_mapper_helpers import S3Writer
            
            s3_config = config.get('s3_output', {})
            bucket = s3_config.get('bucket')
            prefix = s3_config.get('prefix', 'account-map')
            table_name = s3_config.get('table_name', output_table_name)
            
            if not bucket:
                print("❌ S3 bucket not configured for parquet output mode")
                logger.error("S3 bucket not configured")
                return
            
            s3_writer = S3Writer(athena, s3)
            
            try:
                # Write Parquet to S3
                s3_path = s3_writer.write_parquet(account_map, bucket, prefix)
                print(f"✅ Wrote Parquet file to: {s3_path}")
                logger.info("Successfully wrote Parquet to S3: %s", s3_path)
                
                # Create Athena table
                print(f"📊 Creating Athena table: {database_target}.{table_name}...")
                success = s3_writer.create_athena_table(
                    s3_path,
                    table_name,
                    database_target,
                    account_map
                )
                
                if success:
                    print(f"✅ Successfully created table: {database_target}.{table_name}\n")
                    logger.info("Successfully created Athena table")
                else:
                    print(f"❌ Failed to create table: {database_target}.{table_name}\n")
                    logger.error("Failed to create Athena table")
                    return
                
            except Exception as e:
                print(f"❌ Failed to write S3 output: {e}\n")
                logger.error("Failed to write S3 output: %s", str(e), exc_info=True)
                return
        
        else:
            print(f"❌ Unknown output mode: {output_mode}")
            logger.error("Unknown output mode: %s", output_mode)
            return
        
        # Step 5: Display summary
        display_summary({
            'accounts_processed': len(account_map),
            'output_mode': output_mode,
            'database': database_target,
            'table_name': output_table_name if output_mode == 'view' else table_name,
            's3_path': s3_path if output_mode == 'parquet' else None,
            'hierarchy_levels': len(config.get('rules', {}).get('hierarchy_levels', []))
        })
        
        logger.info("="*60)
        logger.info("Account Mapper Workflow Completed Successfully")
        logger.info("="*60)
        
    except Exception as e:
        print(f"\n❌ Error during workflow execution: {e}\n")
        logger.error("Workflow execution failed: %s", str(e), exc_info=True)
        sys.exit(1)

def display_summary(results: dict):
    """
    Display execution summary.
    
    Args:
        results: Dictionary containing execution results:
            - accounts_processed: Number of accounts processed
            - output_mode: Output mode (view or parquet)
            - database: Target database name
            - table_name: Output table/view name
            - s3_path: S3 path (if parquet mode)
            - hierarchy_levels: Number of hierarchy levels configured
    """
    print("\n" + "="*60)
    print("📋 Execution Summary")
    print("="*60)
    print(f"\n✅ Successfully processed {results['accounts_processed']} accounts")
    print(f"📊 Applied {results['hierarchy_levels']} hierarchy level(s)")
    print(f"\n🎯 Output Details:")
    print(f"   Mode: {results['output_mode']}")
    print(f"   Database: {results['database']}")
    print(f"   Table/View: {results['table_name']}")
    
    if results.get('s3_path'):
        print(f"   S3 Location: {results['s3_path']}")
    
    print("\n" + "="*60 + "\n")


