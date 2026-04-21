"""Account Mapper - Interactive account mapping workflow

This module provides high-level commands for the account mapping workflow,
delegating to the detailed implementation in account_mapper_helpers.py
"""

import logging
from cid.helpers import Athena

logger = logging.getLogger(__name__)


class AccountMapper:
    """High-level account mapping operations"""

    def __init__(self, athena: Athena, view_name: str = 'account_map'):
        """
        Initialize AccountMapper.

        Args:
            athena: Athena helper instance
            view_name: Name of the account map view (default: 'account_map')
        """
        self.athena = athena
        self.view_name = view_name

    def create_mapping(self, source_file: str = None, source_database: str = None) -> dict:
        """
        Execute the complete account mapping workflow.

        Args:
            source_file: Optional path to CSV/Excel/JSON file for file-based dimensions
            source_database: Optional source database name (skips discovery if provided)

        Returns:
            dict: Results containing created view names and status
        """
        from cid.helpers.account_mapper_helpers import UnifiedWorkflow

        workflow = UnifiedWorkflow(athena=self.athena, view_name=self.view_name)
        results = workflow.execute(source_file=source_file, source_database=source_database)

        # Display results summary
        if results.get('status') == 'success':
            print("\n" + "="*60)
            print("✅ Account Mapping Complete")
            print("="*60)
            print(f"\nCreated views:")

            # Collect all created views from the results
            created_views = []
            if results.get('file_source_view'):
                created_views.append(results['file_source_view'])
            if results.get('config_view'):
                created_views.append(results['config_view'])
            if results.get('account_map_view'):
                created_views.append(results['account_map_view'])

            if created_views:
                for view in created_views:
                    print(f"  - {view}")
            else:
                print("  (No views were created)")

            print("\n" + "="*60 + "\n")
        elif results.get('status') == 'cancelled':
            print("\n⚠️  Operation cancelled by user\n")
        else:
            print("\n❌ Operation failed\n")

        return results

    def view_config(self, database: str = None) -> None:
        """
        View and optionally modify existing account mapper configuration.

        Args:
            database: Optional database name (will auto-discover if not provided)
        """
        from cid.helpers.account_mapper_helpers import ConfigManager, AutoDiscovery, UnifiedWorkflow
        from InquirerPy import inquirer
        from InquirerPy.base.control import Choice

        # Initialize helpers
        discovery = AutoDiscovery(self.athena)
        config_mgr = ConfigManager(self.athena, self.view_name)

        # Discover database if not provided
        if not database:
            database = discovery.discover_databases()

        print("\n" + "="*60)
        print("⚙️  Account Mapper Configuration Viewer")
        print("="*60 + "\n")

        # Load existing configuration
        config = config_mgr.load_from_view(database)

        if not config:
            print(f"❌ No configuration found for view '{self.view_name}' in database '{database}'")
            print("\nTo create a new account map, use: cid-cmd map\n")
            return

        # Display configuration summary
        print("📋 Current Configuration:\n")
        print(f"Database: {config['metadata'].get('source_database')}")
        print(f"Table: {config['metadata'].get('source_table')}")

        if config.get('file_source') or config['metadata'].get('file_source_view'):
            print(f"File Source View: {config['metadata'].get('file_source_view')}")

        print(f"\nTaxonomy Dimensions ({len(config.get('taxonomy_dimensions', []))}):")
        for dim in config.get('taxonomy_dimensions', []):
            print(f"  - {dim['name']}: {dim['source_type']} = {dim['source_value']}")

        print("\n" + "="*60 + "\n")

        # Offer modification options
        while True:
            action = inquirer.select(
                message="What would you like to do?",
                choices=[
                    Choice(value="regenerate", name="🔄 Regenerate account map with current config"),
                    Choice(value="modify", name="✏️  Modify configuration (create new map)"),
                    Choice(value="exit", name="🚪 Exit")
                ]
            ).execute()

            if action == "regenerate":
                # Regenerate the account map using existing configuration
                confirm = inquirer.confirm(
                    message="This will recreate the account map views. Continue?",
                    default=True
                ).execute()

                if confirm:
                    workflow = UnifiedWorkflow(self.athena, self.view_name)
                    # Execute — workflow will find existing config and offer to reuse it
                    results = workflow.execute()

                    if results.get('status') == 'success':
                        print("\n✅ Account map regenerated successfully\n")
                    else:
                        print("\n❌ Failed to regenerate account map\n")
                break

            elif action == "modify":
                print("\n📝 To modify the configuration, please run: cid-cmd map")
                print("This will start the interactive configuration workflow.\n")
                break

            elif action == "exit":
                break
