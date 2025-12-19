import json
import logging
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import pandas as pd
import boto3

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

from cid.helpers import Athena

logger = logging.getLogger(__name__)


def parse_athena_tags(tags_string: str) -> List[Dict[str, str]]:
    """
    Parse Athena tags from string format to list of dicts.
    
    Athena returns tags in a string format like:
    "[{key=Environment, value=Production}, {key=Team, value=Engineering}]"
    
    This function parses that format into a list of dictionaries:
    [{'key': 'Environment', 'value': 'Production'}, {'key': 'Team', 'value': 'Engineering'}]
    
    Args:
        tags_string: String representation of tags from Athena
        
    Returns:
        List of dictionaries with 'key' and 'value' fields
    """
    if not tags_string or pd.isna(tags_string) or tags_string == '[]':
        return []
    
    try:
        # Remove outer brackets
        tags_string = tags_string.strip()
        if tags_string.startswith('[') and tags_string.endswith(']'):
            tags_string = tags_string[1:-1]
        
        if not tags_string:
            return []
        
        # Parse individual tag items
        tags = []
        # Split by '}, {' to separate individual tags
        tag_items = tags_string.split('}, {')
        
        for item in tag_items:
            # Clean up the item
            item = item.strip().strip('{}')
            if not item:
                continue
            
            # Parse key=value pairs
            tag_dict = {}
            parts = item.split(', ')
            for part in parts:
                if '=' in part:
                    key, value = part.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    tag_dict[key] = value
            
            if tag_dict:
                tags.append(tag_dict)
        
        return tags
        
    except Exception as e:
        logger.warning("Failed to parse Athena tags string '%s': %s", tags_string, str(e))
        return []


# Session-level cache for Athena data
_athena_data_cache: Dict[str, Any] = {
    'data': None,  # Cached DataFrame (full data for tag extraction)
    'preview_data': None,  # Cached preview data (list of dicts, LIMIT 10)
    'columns': None,  # Cached column list
    'tag_keys': {},  # Cached tag keys by column name
    'config_hash': None  # Hash of config to detect changes
}


class Account:
    """
    Represents an AWS account with metadata and business hierarchy information.
    
    The Account class ensures that account IDs are properly formatted (12 digits with
    leading zeros) and provides class-level storage for all account instances to enable
    easy lookup and export.
    
    Attributes:
        _account_id (str): 12-digit account ID
        _account_name (str): Account name
        _payer_account_id (str): Payer account ID (12 digits)
        _payer_account_name (str): Payer account name
        _account_tags (dict): Account tags
        _business_unit (dict): Business unit hierarchy information
        
    Class Attributes:
        _all_accounts (dict): Class-level storage of all Account instances
    """
    
    _all_accounts: Dict[str, Dict] = {}
    
    def __init__(self, account_id: str):
        """
        Initialize an Account instance with a validated account ID.
        
        Args:
            account_id: AWS account ID (will be zero-padded to 12 digits)
            
        Raises:
            TypeError: If account_id contains non-numeric characters
        """
        self._account_name = ""
        self._account_tags = {}
        self._business_unit = {}
        self._payer_account_id = ""
        self._payer_account_name = ""
        
        self.set_account_id(account_id)
    
    def set_account_id(self, account_id: str) -> None:
        """
        Set and validate the account ID, ensuring 12-digit format with leading zeros.
        
        Args:
            account_id: Account ID as string or int
            
        Raises:
            TypeError: If account_id contains non-numeric characters
        """
        if isinstance(account_id, int):
            # Convert to string and ensure 12 digits
            self._account_id = str(account_id).zfill(12)
        elif isinstance(account_id, str):
            # Validate that all characters are digits
            try:
                # Test if we can interpret each character as an integer
                [int(x) for x in account_id]
                self._account_id = account_id.zfill(12)
            except ValueError as e:
                raise TypeError('Account ID must consist of 12 digits from 0-9.') from e
        else:
            raise TypeError('Account ID must be a string or integer')
        
        # Initialize entry in class-level storage
        if self._account_id not in Account._all_accounts:
            Account._all_accounts[self._account_id] = {}
    
    def get_account_id(self) -> str:
        """
        Get the account ID.
        
        Returns:
            12-digit account ID as string
        """
        return self._account_id
    
    def set_account_name(self, account_name: str) -> None:
        """
        Set the account name.
        
        Args:
            account_name: Name of the account
        """
        self._account_name = str(account_name)
        Account._all_accounts[self._account_id]["account_name"] = self._account_name
    
    def get_account_name(self) -> str:
        """
        Get the account name.
        
        Returns:
            Account name
        """
        return self._account_name
    
    def set_payer_id(self, payer_id: str) -> None:
        """
        Set the payer account ID with validation and zero-padding.
        
        Args:
            payer_id: Payer account ID as string or int
            
        Raises:
            TypeError: If payer_id contains non-numeric characters
        """
        if isinstance(payer_id, int):
            self._payer_account_id = str(payer_id).zfill(12)
        elif isinstance(payer_id, str):
            try:
                # Validate that all characters are digits
                [int(x) for x in payer_id]
                self._payer_account_id = payer_id.zfill(12)
            except ValueError as e:
                raise TypeError('Payer Account ID must consist of 12 digits from 0-9.') from e
        else:
            raise TypeError('Payer Account ID must be a string or integer')
        
        Account._all_accounts[self._account_id]["payer_id"] = self._payer_account_id
    
    def get_payer_id(self) -> str:
        """
        Get the payer account ID.
        
        Returns:
            12-digit payer account ID as string
        """
        return self._payer_account_id
    
    def set_payer_name(self, payer_name: str) -> None:
        """
        Set the payer account name.
        
        Args:
            payer_name: Name of the payer account
        """
        self._payer_account_name = str(payer_name)
        Account._all_accounts[self._account_id]["payer_name"] = self._payer_account_name
    
    def get_payer_name(self) -> str:
        """
        Get the payer account name.
        
        Returns:
            Payer account name
        """
        return self._payer_account_name
    
    def add_tag(self, tag_name: str, tag_value: str) -> None:
        """
        Add a tag to the account.
        
        Args:
            tag_name: Tag key
            tag_value: Tag value
        """
        self._account_tags[tag_name] = tag_value
        Account._all_accounts[self._account_id]["tags"] = self._account_tags
    
    def get_tags(self) -> Dict[str, str]:
        """
        Get all tags for this account.
        
        Returns:
            Dictionary of tag key-value pairs
        """
        return self._account_tags
    
    def get_tag(self, tag_name: str) -> Optional[str]:
        """
        Get a specific tag value.
        
        Args:
            tag_name: Tag key to retrieve
            
        Returns:
            Tag value or None if not found
        """
        return self._account_tags.get(tag_name)
    
    def set_business_unit(self, bu_name: str, bu_value: str) -> None:
        """
        Set a business unit hierarchy level for this account.
        
        Args:
            bu_name: Business unit level name (e.g., 'level_1', 'level_2')
            bu_value: Business unit value
        """
        self._business_unit[bu_name] = bu_value
        Account._all_accounts[self._account_id][bu_name] = bu_value
    
    def get_business_unit(self) -> Dict[str, str]:
        """
        Get all business unit information for this account.
        
        Returns:
            Dictionary of business unit levels and values
        """
        return self._business_unit
    
    @classmethod
    def get_all_accounts(cls) -> Dict[str, Dict]:
        """
        Get all Account instances and their metadata.
        
        Returns:
            Dictionary with account IDs as keys and metadata dictionaries as values
        """
        return cls._all_accounts
    
    @classmethod
    def reset_all_accounts(cls) -> None:
        """
        Clear all stored account data.
        
        This is useful for testing or when starting a new transformation run.
        """
        cls._all_accounts = {}
    
    @classmethod
    def to_dataframe(cls) -> pd.DataFrame:
        """
        Convert all accounts to a pandas DataFrame.
        
        Returns:
            DataFrame with account_id as index and all metadata as columns
        """
        if not cls._all_accounts:
            return pd.DataFrame()
        
        # Build a list of records with account_id included
        records = []
        for account_id, metadata in cls._all_accounts.items():
            record = {'account_id': account_id}
            record.update(metadata)
            records.append(record)
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        return df
    
    # Properties for convenient access
    account_id = property(get_account_id, set_account_id)
    account_name = property(get_account_name, set_account_name)
    payer_id = property(get_payer_id, set_payer_id)
    payer_name = property(get_payer_name, set_payer_name)


class TransformEngine:
    """
    Orchestrates data transformation according to configured rules.
    
    The TransformEngine applies hierarchy rules to organization data, creating
    Account instances with business unit information and converting them to a
    DataFrame for output.
    
    Attributes:
        config: Configuration dictionary containing transformation rules
        org_data: DataFrame containing organization data from Athena
        file_data: Optional DataFrame containing external file data
    """
    
    def __init__(self, config: dict, org_data: pd.DataFrame, file_data: Optional[pd.DataFrame] = None):
        """
        Initialize TransformEngine with configuration and data.
        
        Args:
            config: Configuration dictionary with rules section
            org_data: DataFrame containing organization data
            file_data: Optional DataFrame containing external file data for file-based rules
        """
        self.config = config
        self.org_data = org_data
        self.file_data = file_data
        
        logger.info("Initialized TransformEngine with %d accounts", len(org_data))
    
    def transform(self) -> pd.DataFrame:
        """
        Execute all transformation rules and return account map.
        
        This method orchestrates the complete transformation process:
        1. Reset any existing Account instances
        2. Create Account instances for each account in org_data
        3. Set basic account information (name, payer)
        4. Apply all hierarchy rules
        5. Convert to DataFrame
        
        Returns:
            DataFrame containing account map with all hierarchy levels
            
        Raises:
            ValueError: If configuration is invalid or required data is missing
        """
        logger.info("Starting transformation process")
        
        # Reset Account class storage
        Account.reset_all_accounts()
        
        # Validate that we have organization data
        if self.org_data.empty:
            logger.warning("Organization data is empty")
            return pd.DataFrame()
        
        # Process each account
        account_count = 0
        for _, row in self.org_data.iterrows():
            account_id = str(row.get('id', '')).zfill(12)
            
            if not account_id or account_id == '000000000000':
                logger.warning("Skipping row with invalid account ID")
                continue
            
            try:
                # Create Account instance
                account = Account(account_id)
                
                # Set basic account information
                account_name = row.get('name', '')
                if account_name:
                    account.set_account_name(str(account_name))
                
                # Set payer information
                payer_id = row.get('payer_id', '')
                if payer_id:
                    account.set_payer_id(str(payer_id))
                    # Note: payer_name is only set if used in a hierarchy rule
                
                account_count += 1
                
            except Exception as e:
                logger.error(
                    "Error creating account %s: %s",
                    account_id, str(e),
                    exc_info=True
                )
                continue
        
        logger.info("Created %d Account instances", account_count)
        
        # Apply hierarchy rules
        self.apply_hierarchy_rules()
        
        # Convert to DataFrame
        result_df = Account.to_dataframe()
        logger.info("Transformation complete. Generated %d rows", len(result_df))
        
        return result_df
    
    def apply_hierarchy_rules(self) -> None:
        """
        Apply all configured hierarchy level rules.
        
        This method iterates through all configured hierarchy levels and applies
        the appropriate extraction rule for each account.
        """
        rules_config = self.config.get('rules', {})
        hierarchy_levels = rules_config.get('hierarchy_levels', [])
        
        if not hierarchy_levels:
            logger.warning("No hierarchy levels configured")
            return
        
        logger.info("Applying %d hierarchy rules", len(hierarchy_levels))
        
        # Sort levels by level number to ensure correct order
        sorted_levels = sorted(hierarchy_levels, key=lambda x: x.get('level', 0))
        
        # Get all accounts
        all_accounts = Account.get_all_accounts()
        
        # Apply each rule to each account
        for level_config in sorted_levels:
            level_num = level_config.get('level')
            level_name = level_config.get('name', f'level_{level_num}')
            
            logger.debug("Applying rule for %s", level_name)
            
            success_count = 0
            for account_id in all_accounts.keys():
                try:
                    # Apply the rule for this level
                    value = self.apply_single_rule(level_config, account_id)
                    
                    if value is not None:
                        # Create Account instance to set the business unit
                        account = Account(account_id)
                        account.set_business_unit(level_name, value)
                        success_count += 1
                    
                except Exception as e:
                    logger.error(
                        "Error applying rule %s to account %s: %s",
                        level_name, account_id, str(e),
                        exc_info=True
                    )
            
            logger.info(
                "Applied rule %s: %d/%d accounts successful",
                level_name, success_count, len(all_accounts)
            )
    
    def apply_single_rule(self, rule: Dict[str, Any], account_id: str) -> Optional[str]:
        """
        Apply a single transformation rule to an account.
        
        This method determines the rule source type and calls the appropriate
        extraction function with the configured parameters.
        
        Args:
            rule: Rule configuration dictionary containing source and parameters
            account_id: 12-digit account ID to apply rule to
            
        Returns:
            Extracted value if successful, None otherwise
            
        Raises:
            ValueError: If rule source type is unknown or required parameters are missing
        """
        source = rule.get('source')
        parameters = rule.get('parameters', {})
        
        if not source:
            logger.error("Rule missing 'source' field: %s", rule)
            return None
        
        try:
            if source == 'athena_tags':
                # Extract from hierarchytags
                tag_key = parameters.get('tag_key')
                if not tag_key:
                    logger.error("athena_tags rule missing 'tag_key' parameter")
                    return None
                
                return extract_from_tag(self.org_data, account_id, tag_key)
            
            elif source == 'athena_name':
                # Extract from account name split
                separator = parameters.get('separator')
                index = parameters.get('index')
                
                if separator is None or index is None:
                    logger.error("athena_name rule missing 'separator' or 'index' parameter")
                    return None
                
                return extract_from_account_name(
                    self.org_data,
                    account_id,
                    separator,
                    int(index)
                )
            
            elif source == 'athena_payer':
                # Extract payer information
                # Default to payer name, but allow configuration
                info_type = parameters.get('info_type', 'name')
                payer_names = parameters.get('payer_names', None)
                return extract_payer_info(self.org_data, account_id, info_type, payer_names)
            
            elif source == 'file':
                # Extract from external file
                if self.file_data is None:
                    logger.error("File source specified but no file data loaded")
                    return None
                
                column_name = parameters.get('column_name')
                account_id_column = parameters.get('account_id_column')
                
                if not column_name or not account_id_column:
                    logger.error("file rule missing 'column_name' or 'account_id_column' parameter")
                    return None
                
                return extract_from_file(
                    self.file_data,
                    account_id,
                    column_name,
                    account_id_column
                )
            
            else:
                logger.error("Unknown rule source type: %s", source)
                return None
        
        except Exception as e:
            logger.error(
                "Error applying rule with source %s to account %s: %s",
                source, account_id, str(e),
                exc_info=True
            )
            return None

class DataLoader:
    """Loads organization data from Athena or file sources."""
    
    def __init__(self, athena: Athena, config: dict):
        """
        Initialize DataLoader with Athena helper and configuration.
        
        Args:
            athena: Athena helper instance
            config: Configuration dictionary containing data source settings
        """
        self.athena = athena
        self.config = config
        logger.info("Initialized DataLoader")
    
    def load_from_athena(self) -> pd.DataFrame:
        """
        Load organization data from Athena using CID helper.
        
        Returns:
            DataFrame containing organization data with columns:
            id, name, hierarchy, hierarchytags, payer_id, parenttags
            
        Raises:
            ValueError: If Athena configuration is missing or invalid
            RuntimeError: If Athena query fails
        """
        athena_config = self.config.get('athena', {})
        database = athena_config.get('database')
        table = athena_config.get('table')
        
        if not database or not table:
            raise ValueError("Athena database and table must be configured")
        
        logger.info("Loading organization data from Athena: %s.%s", database, table)
        
        try:
            # Build query to select organization data
            query = f"""
                SELECT 
                    id,
                    name,
                    hierarchy,
                    hierarchytags,
                    payer_id,
                    parenttags
                FROM "{database}"."{table}"
            """
            
            # Execute query using CID Athena helper
            # The query method returns a list of rows with header as first row
            results = self.athena.query(
                sql=query,
                database=database,
                include_header=True
            )
            
            # Convert results to DataFrame
            if not results or len(results) < 2:
                logger.warning("No data returned from Athena query")
                return pd.DataFrame()
            
            # First row is header, rest are data
            header = results[0]
            data = results[1:]
            df = pd.DataFrame(data, columns=header)
            
            # Parse hierarchytags column from string format to list of dicts
            if 'hierarchytags' in df.columns:
                logger.info("Parsing hierarchytags column from Athena string format")
                df['hierarchytags'] = df['hierarchytags'].apply(
                    lambda x: parse_athena_tags(x) if pd.notna(x) else []
                )
            
            # Parse parenttags column if present
            if 'parenttags' in df.columns:
                logger.info("Parsing parenttags column from Athena string format")
                df['parenttags'] = df['parenttags'].apply(
                    lambda x: parse_athena_tags(x) if pd.notna(x) else []
                )
            
            logger.info("Successfully loaded %d accounts from Athena", len(df))
            return df
            
        except Exception as e:
            logger.error("Failed to load data from Athena: %s", str(e), exc_info=True)
            raise RuntimeError(f"Athena query failed: {e}") from e
    
    def load_from_file(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Load data from file (Excel, CSV, or JSON).
        
        Args:
            file_path: Path to file. If None, uses path from configuration.
            
        Returns:
            DataFrame containing file data
            
        Raises:
            ValueError: If file path is not provided or configured
            FileNotFoundError: If file does not exist
            ValueError: If file format is not supported
        """
        # Use provided path or get from configuration
        if file_path is None:
            file_source_config = self.config.get('file_source', {})
            file_path = file_source_config.get('file_path')
        
        if not file_path:
            raise ValueError("File path must be provided or configured in file_source.file_path")
        
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info("Loading data from file: %s", file_path)
        
        # Determine file format from extension
        suffix = file_path_obj.suffix.lower()
        
        try:
            if suffix == '.json':
                df = pd.read_json(file_path)
                logger.info("Loaded %d records from JSON file", len(df))
            elif suffix == '.csv':
                df = pd.read_csv(file_path)
                logger.info("Loaded %d records from CSV file", len(df))
            elif suffix in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
                logger.info("Loaded %d records from Excel file", len(df))
            else:
                raise ValueError(
                    f"Unsupported file format: {suffix}. "
                    "Supported formats: .json, .csv, .xlsx, .xls"
                )
            
            return df
            
        except Exception as e:
            logger.error("Failed to load file %s: %s", file_path, str(e), exc_info=True)
            raise
    
    def get_available_tag_keys(self, org_data: Optional[pd.DataFrame] = None, tag_column: str = 'hierarchytags') -> list:
        """
        Extract available tag keys from hierarchytags column.
        
        Args:
            org_data: DataFrame containing organization data with hierarchytags column.
                     If None, loads data from Athena.
            tag_column: Name of the column containing tags (default: 'hierarchytags')
        
        Returns:
            List of unique tag keys found in hierarchytags
            
        Raises:
            ValueError: If tag column is not present
        """
        # Load data if not provided
        if org_data is None:
            logger.info("Loading organization data to extract tag keys")
            org_data = self.load_from_athena()
        
        if tag_column not in org_data.columns:
            raise ValueError(f"{tag_column} column not found in organization data")
        
        logger.info("Extracting available tag keys from %s", tag_column)
        
        # Extract unique keys from hierarchytags
        tag_keys = set()
        
        for tags_value in org_data[tag_column].dropna():
            # Handle different formats
            if isinstance(tags_value, str):
                # Parse Athena string format
                parsed_tags = parse_athena_tags(tags_value)
                for tag_item in parsed_tags:
                    if 'key' in tag_item:
                        tag_keys.add(tag_item['key'])
            elif isinstance(tags_value, list):
                # Already a list of dicts
                for tag_item in tags_value:
                    if isinstance(tag_item, dict) and 'key' in tag_item:
                        tag_keys.add(tag_item['key'])
        
        tag_keys_list = sorted(list(tag_keys))
        logger.info("Found %d unique tag keys", len(tag_keys_list))
        
        return tag_keys_list
    
    def get_available_columns(self) -> List[str]:
        """
        Get list of available columns from the Athena table.
        
        Returns:
            List of column names
            
        Raises:
            ValueError: If Athena configuration is missing
            RuntimeError: If query fails
        """
        athena_config = self.config.get('athena', {})
        database = athena_config.get('database')
        table = athena_config.get('table')
        
        if not database or not table:
            raise ValueError("Athena database and table must be configured")
        
        logger.info("Getting columns from Athena table: %s.%s", database, table)
        
        try:
            # Query to get column names (limit 0 to just get schema)
            query = f'SELECT * FROM "{database}"."{table}" LIMIT 0'
            
            results = self.athena.query(
                sql=query,
                database=database,
                include_header=True
            )
            
            if results and len(results) > 0:
                columns = results[0]  # First row is header
                logger.info("Found %d columns", len(columns))
                return columns
            else:
                logger.warning("No columns returned")
                return []
                
        except Exception as e:
            logger.error("Failed to get columns from Athena: %s", str(e), exc_info=True)
            raise RuntimeError(f"Failed to get columns: {e}") from e

class ConfigManager:
    """Manages application configuration loading, saving, and validation."""
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file (default: config.json)
        """
        self.config_path = Path(config_path)
        self._config: Optional[dict] = None
    
    def load(self) -> dict:
        """
        Load configuration from file.
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            json.JSONDecodeError: If configuration file is invalid JSON
        """
        try:
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return self._config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def save(self, config: dict) -> bool:
        """
        Save configuration to file.
        
        Args:
            config: Configuration dictionary to save
            
        Returns:
            True if save was successful, False otherwise
        """
        try:
            # Ensure parent directory exists
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write configuration with pretty formatting
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)
            
            self._config = config
            logger.info(f"Configuration saved to {self.config_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            return False
    
    def exists(self) -> bool:
        """
        Check if configuration file exists.
        
        Returns:
            True if configuration file exists, False otherwise
        """
        return self.config_path.exists()
    
    def is_valid(self) -> bool:
        """
        Validate current configuration.
        
        Returns:
            True if configuration is valid, False otherwise
        """        
        if not self.exists():
            return False
        
        try:
            config = self.load()
            is_valid, errors = validate_config(config)
            
            if not is_valid:
                logger.warning(f"Configuration validation failed: {errors}")
            
            return is_valid
        except Exception as e:
            logger.error(f"Error validating configuration: {e}")
            return False
    
    def get_missing_fields(self) -> list:
        """
        Get list of missing required fields in configuration.
        
        Returns:
            List of missing field paths (e.g., ['general.aws_region', 'athena.database'])
        """
        
        if not self.exists():
            return ["Configuration file does not exist"]
        
        try:
            config = self.load()
            is_valid, errors = validate_config(config)
            return errors if not is_valid else []
        except Exception as e:
            logger.error(f"Error checking missing fields: {e}")
            return [str(e)]

class AthenaWriter:
    """
    Writes account map DataFrames to Athena as views.
    
    The AthenaWriter handles creating Athena views from DataFrames, with automatic
    splitting when SQL statements exceed Athena's size limits. It uses the CID
    Athena helper for all Athena operations.
    
    Attributes:
        config (dict): Application configuration
        athena_helper (Athena): CID Athena helper instance
        MAX_SQL_SIZE (int): Maximum SQL statement size in bytes (262144)
    """
    
    MAX_SQL_SIZE = 262144  # Athena's maximum SQL statement size in bytes
    
    def __init__(self, config: dict, athena_helper: Athena = None):
        """
        Initialize AthenaWriter with configuration and optional Athena helper.
        
        Args:
            config: Application configuration dictionary containing:
                - general.aws_region: AWS region for Athena
                - athena.database_target: Target database for views
                - athena.catalog: Athena catalog name
                - athena.workgroup: Athena workgroup
            athena_helper: Optional Athena helper instance from cid-cmd.
                          If not provided, creates a new instance.
        """
        self.config = config
        
        if athena_helper:
            # Use provided Athena helper from cid-cmd
            self.athena_helper = athena_helper
            logger.info("AthenaWriter using provided Athena helper instance")
        else:
            # Create new Athena helper instance
            region = config['general']['aws_region']
            session = boto3.Session(region_name=region)
            self.athena_helper = Athena(session=session)

            # Set workgroup and catalog from config to avoid interactive prompts
            workgroup = config.get('athena', {}).get('workgroup', 'primary')
            self.athena_helper._WorkGroup = workgroup
            catalog = config.get('athena', {}).get('catalog', 'AwsDataCatalog')
            self.athena_helper._CatalogName = catalog

            logger.info("AthenaWriter initialized with Athena helper for workgroup: %s, catalog: %s", workgroup, catalog)
    
    def write_as_view(self, df: pd.DataFrame, view_name: str) -> bool:
        """
        Write DataFrame as Athena view.
        
        This method creates an Athena view. If no file sources are used in the
        configuration, it generates SQL that transforms the source table directly.
        Otherwise, it falls back to materializing the data with VALUES clauses.
        
        Args:
            df: DataFrame to write as view
            view_name: Name for the Athena view
            
        Returns:
            True if view creation was successful, False otherwise
            
        Example:
            >>> writer = AthenaWriter(config)
            >>> df = pd.DataFrame({'account_id': ['123456789012'], 'name': ['Test']})
            >>> writer.write_as_view(df, 'account_map')
            True
        """
        database = self.config['athena'].get('database_target', 
                                              self.config['athena']['database'])
        
        logger.info("Writing DataFrame to Athena view: %s.%s", database, view_name)
        logger.info("DataFrame shape: %s", df.shape)
        
        try:
            # Check if any hierarchy rules use file sources
            uses_file_source = self._uses_file_source()
            
            if not uses_file_source:
                # Generate SQL transformation view
                logger.info("No file sources detected, generating SQL transformation view")
                return self._create_transformation_view(view_name, database)
            else:
                # Fall back to VALUES-based approach
                logger.info("File sources detected, using VALUES-based approach")
                view_names = self.create_view_from_values(df, view_name, database)
                
                if len(view_names) == 1:
                    # Single view created successfully
                    logger.info("Successfully created view: %s.%s", database, view_name)
                    return True
                else:
                    # Multiple views created, need to create UNION view
                    logger.info("Created %d split views, creating UNION view", len(view_names))
                    success = self.create_union_view(view_names, view_name, database)
                    
                    if success:
                        logger.info("Successfully created UNION view: %s.%s", database, view_name)
                    else:
                        logger.error("Failed to create UNION view: %s.%s", database, view_name)
                    
                    return success
                
        except Exception as e:
            logger.error("Failed to write view %s: %s", view_name, str(e), exc_info=True)
            return False
    
    def _uses_file_source(self) -> bool:
        """
        Check if any hierarchy rules use file sources.
        
        Returns:
            True if any rule uses 'file' source, False otherwise
        """
        rules = self.config.get('rules', {}).get('hierarchy_levels', [])
        return any(rule.get('source') == 'file' for rule in rules)
    
    def _create_transformation_view(self, view_name: str, database: str) -> bool:
        """
        Create an Athena view using SQL transformations on the source table.
        
        This generates a CREATE OR REPLACE VIEW statement that transforms the
        source table according to the configured hierarchy rules.
        
        Args:
            view_name: Name for the view
            database: Target database name
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n🔧 DEBUG: _create_transformation_view called for {database}.{view_name}")
        try:
            # Always try to drop any existing view or table first
            # This is simpler and more reliable than checking existence
            print(f"🔧 DEBUG: About to drop existing view/table")
            logger.info("Attempting to drop any existing view or table: %s.%s", database, view_name)
            
            # Try dropping as table first (more common), then as view
            dropped = False
            
            try:
                print(f"�  DEBUG: Trying to drop as table")
                self._drop_table(view_name, database)
                print(f"�️D  Dropped existing table {database}.{view_name}")
                dropped = True
            except Exception as e:
                print(f"🔧 DEBUG: Drop table failed or no table exists: {e}")
                logger.debug("No table to drop (or drop failed): %s", str(e))
            
            if not dropped:
                try:
                    print(f"🔧 DEBUG: Trying to drop as view")
                    self._drop_view(view_name, database)
                    print(f"🗑️  Dropped existing view {database}.{view_name}")
                    dropped = True
                except Exception as e:
                    print(f"🔧 DEBUG: Drop view failed or no view exists: {e}")
                    logger.debug("No view to drop (or drop failed): %s", str(e))
            
            if not dropped:
                print(f"ℹ️  No existing view or table to drop")
            
            print()
            
            sql = self._generate_transformation_sql(view_name, database)
            logger.info("Generated transformation SQL (%d bytes)", self.calculate_sql_size(sql))
            logger.debug("SQL: %s", sql)
            
            self._execute_view_creation(sql, database)
            logger.info("Successfully created transformation view: %s.%s", database, view_name)
            return True
            
        except Exception as e:
            logger.error("Failed to create transformation view: %s", str(e), exc_info=True)
            return False
    
    def _view_exists(self, view_name: str, database: str) -> bool:
        """
        Check if a view or table exists in the database.
        
        Args:
            view_name: Name of the view/table
            database: Database name
            
        Returns:
            True if view or table exists, False otherwise
        """
        try:
            # Check if it's a view
            check_sql = f"SHOW VIEWS IN {database} LIKE '{view_name}'"
            results = self.athena_helper.query(
                sql=check_sql,
                database=database,
                include_header=False
            )
            
            if len(results) > 0:
                return True
            
            # Check if it's a table
            check_sql = f"SHOW TABLES IN {database} LIKE '{view_name}'"
            results = self.athena_helper.query(
                sql=check_sql,
                database=database,
                include_header=False
            )
            
            return len(results) > 0
            
        except Exception as e:
            logger.warning("Could not check if view/table exists: %s", str(e))
            # If we can't check, assume it doesn't exist
            return False
    
    def _is_table(self, view_name: str, database: str) -> bool:
        """
        Check if the object is a table (not a view).
        
        Args:
            view_name: Name of the object
            database: Database name
            
        Returns:
            True if it's a table, False if it's a view or doesn't exist
        """
        try:
            check_sql = f"SHOW TABLES IN {database} LIKE '{view_name}'"
            results = self.athena_helper.query(
                sql=check_sql,
                database=database,
                include_header=False
            )
            
            return len(results) > 0
            
        except Exception as e:
            logger.warning("Could not check if table exists: %s", str(e))
            return False
    
    def _drop_view(self, view_name: str, database: str) -> None:
        """
        Drop a view from the database.
        
        Args:
            view_name: Name of the view
            database: Database name
            
        Raises:
            Exception: If drop fails
        """
        try:
            drop_sql = f"DROP VIEW IF EXISTS {database}.{view_name}"
            logger.info("Executing: %s", drop_sql)
            self.athena_helper.query(
                sql=drop_sql,
                database=database
            )
            logger.info("Successfully dropped view %s.%s", database, view_name)
        except Exception as e:
            logger.error("Failed to drop view %s.%s: %s", database, view_name, str(e))
            raise
    
    def _drop_table(self, table_name: str, database: str) -> None:
        """
        Drop a table from the database.
        
        Args:
            table_name: Name of the table
            database: Database name
            
        Raises:
            Exception: If drop fails
        """
        try:
            drop_sql = f"DROP TABLE IF EXISTS {database}.{table_name}"
            logger.info("Executing: %s", drop_sql)
            self.athena_helper.query(
                sql=drop_sql,
                database=database
            )
            logger.info("Successfully dropped table %s.%s", database, table_name)
        except Exception as e:
            logger.error("Failed to drop table %s.%s: %s", database, table_name, str(e))
            raise
    
    def _generate_transformation_sql(self, view_name: str, database: str) -> str:
        """
        Generate SQL for transformation view.
        
        This creates a SELECT statement that transforms the source table
        according to the configured hierarchy rules.
        
        Args:
            view_name: Name for the view
            database: Target database name
            
        Returns:
            Complete CREATE OR REPLACE VIEW SQL statement
        """
        source_database = self.config['athena']['database']
        source_table = self.config['athena']['table']
        rules = self.config.get('rules', {}).get('hierarchy_levels', [])
        
        # Sort rules by level
        sorted_rules = sorted(rules, key=lambda x: x.get('level', 0))
        
        # Build SELECT clause with base columns and hierarchy transformations
        select_parts = [
            'base.id AS "account_id"',
            'base.name AS "account_name"',
            'base.payer_id AS "payer_id"'
        ]
        
        # Collect tag-based rules for special handling
        tag_rules = []
        non_tag_rules = []
        
        for rule in sorted_rules:
            if rule.get('source') == 'athena_tags':
                tag_rules.append(rule)
            else:
                non_tag_rules.append(rule)
        
        # Add non-tag hierarchy level transformations
        for rule in non_tag_rules:
            level_name = rule.get('name', f"level_{rule.get('level')}")
            source = rule.get('source')
            parameters = rule.get('parameters', {})
            
            sql_expr = self._generate_column_expression(source, parameters, 'base')
            select_parts.append(f'{sql_expr} AS "{level_name}"')
        
        # Add tag-based columns using LEFT JOINs
        for rule in tag_rules:
            level_name = rule.get('name', f"level_{rule.get('level')}")
            tag_key = rule.get('parameters', {}).get('tag_key', '')
            alias = f"tag_{rule.get('level', 0)}"
            select_parts.append(f'{alias}.value AS "{level_name}"')
        
        # Build FROM clause with JOINs for tags
        from_clause = f'FROM "{source_database}"."{source_table}" base'
        
        for rule in tag_rules:
            tag_key = rule.get('parameters', {}).get('tag_key', '')
            alias = f"tag_{rule.get('level', 0)}"
            from_clause += f"""
LEFT JOIN (
    SELECT id, tag.value
    FROM "{source_database}"."{source_table}"
    CROSS JOIN UNNEST(hierarchytags) AS t(tag)
    WHERE tag.key = '{tag_key}'
) {alias} ON base.id = {alias}.id"""
        
        # Build complete SQL
        select_clause = ',\n    '.join(select_parts)
        
        # Use CREATE VIEW (not CREATE OR REPLACE) since we handle dropping separately
        sql = f"""CREATE VIEW {database}.{view_name} AS
SELECT
    {select_clause}
{from_clause}
"""
        
        return sql
    
    def _generate_column_expression(self, source: str, parameters: dict, table_alias: str = '') -> str:
        """
        Generate SQL expression for a hierarchy level based on source type.
        
        Args:
            source: Source type ('athena_tags', 'athena_name', 'athena_payer')
            parameters: Parameters for the source
            table_alias: Table alias to use (e.g., 'base')
            
        Returns:
            SQL expression string
        """
        prefix = f"{table_alias}." if table_alias else ""
        
        if source == 'athena_tags':
            # This should not be called for athena_tags when using JOINs
            # But if it is, return NULL
            logger.warning("athena_tags should be handled via JOINs, not expressions")
            return "NULL"
        
        elif source == 'athena_name':
            # Split account name and extract part
            separator = parameters.get('separator', '-')
            index = parameters.get('index', 0)
            # SQL uses 1-based indexing
            sql_index = index + 1
            return f"split_part({prefix}name, '{separator}', {sql_index})"
        
        elif source == 'athena_payer':
            # Handle payer information
            use_custom_names = parameters.get('use_custom_names', False)
            payer_names = parameters.get('payer_names', {})
            
            if use_custom_names and payer_names:
                # Generate CASE statement for custom payer names
                case_parts = []
                for payer_id, payer_name in payer_names.items():
                    escaped_name = payer_name.replace("'", "''")
                    case_parts.append(f"WHEN {prefix}payer_id = '{payer_id}' THEN '{escaped_name}'")
                
                case_clause = '\n        '.join(case_parts)
                return f"""CASE
        {case_clause}
        ELSE {prefix}payer_id
    END"""
            else:
                # Just return payer_id
                return f"{prefix}payer_id"
        
        else:
            logger.warning("Unknown source type: %s", source)
            return "NULL"

    def create_view_from_values(
        self,
        df: pd.DataFrame,
        view_name: str,
        database: str
    ) -> List[str]:
        """
        Create Athena view(s) using VALUES clause, splitting if needed.

        This method generates SQL CREATE VIEW statements using the VALUES clause.
        If the SQL exceeds MAX_SQL_SIZE, it splits the DataFrame into chunks
        and creates multiple views with numeric suffixes.

        Args:
            df: DataFrame to convert to view
            view_name: Base name for the view(s)
            database: Target database name

        Returns:
            List of created view names (single item if no split, multiple if split)

        Raises:
            Exception: If view creation fails
        """
        # Generate column list for the view
        columns = df.columns.tolist()

        # Drop existing view/table first (since Athena doesn't support CREATE OR REPLACE VIEW)
        self._safe_drop_view_or_table(view_name, database)

        # Try to create a single view first
        sql = self._generate_values_sql(df, view_name, database, columns)
        sql_size = self.calculate_sql_size(sql)

        if sql_size <= self.MAX_SQL_SIZE:
            # SQL fits in single view
            logger.info("Creating single view (SQL size: %d bytes)", sql_size)
            self._execute_view_creation(sql, database)
            return [view_name]
        else:
            # Need to split into multiple views
            logger.warning("SQL size (%d bytes) exceeds limit (%d bytes)", sql_size, self.MAX_SQL_SIZE)
            logger.info("Splitting DataFrame into multiple views")

            return self._create_split_views(df, view_name, database, columns)

    def _safe_drop_view_or_table(self, name: str, database: str) -> None:
        """
        Safely drop a view or table if it exists.

        Args:
            name: Name of the view/table to drop
            database: Database name
        """
        # Try dropping as view first
        try:
            self._drop_view(name, database)
            logger.info("Dropped existing view %s.%s", database, name)
            return
        except Exception as e:
            logger.debug("No view to drop or drop failed: %s", str(e))

        # Try dropping as table
        try:
            self._drop_table(name, database)
            logger.info("Dropped existing table %s.%s", database, name)
        except Exception as e:
            logger.debug("No table to drop or drop failed: %s", str(e))
    
    def _generate_values_sql(
        self,
        df: pd.DataFrame,
        view_name: str,
        database: str,
        columns: List[str]
    ) -> str:
        """
        Generate CREATE VIEW SQL with VALUES clause.

        All values are treated as VARCHAR strings for consistency with Athena.
        Column names are quoted to handle special characters and reserved words.

        Args:
            df: DataFrame to convert
            view_name: Name for the view
            database: Target database
            columns: List of column names

        Returns:
            Complete SQL statement as string
        """
        # Generate VALUES rows - treat all values as strings for Athena compatibility
        values_rows = []
        for _, row in df.iterrows():
            values = []
            for col in columns:
                value = row[col]
                if pd.isna(value) or value is None or str(value).strip() == '':
                    values.append('NULL')
                else:
                    # Convert everything to string and escape single quotes
                    escaped_value = str(value).replace("'", "''")
                    values.append(f"'{escaped_value}'")

            values_rows.append(f"({', '.join(values)})")

        values_clause = ',\n  '.join(values_rows)

        # Quote column names to handle spaces, special chars, and reserved words
        quoted_columns = [f'"{col}"' for col in columns]
        column_list = ', '.join(quoted_columns)

        # Use CREATE VIEW (not CREATE OR REPLACE - Athena doesn't support it)
        # The caller should handle dropping existing views first
        sql = f"""CREATE VIEW {database}.{view_name} AS
SELECT * FROM (
  VALUES
  {values_clause}
) AS t ({column_list})"""

        return sql
    
    def _create_split_views(
        self,
        df: pd.DataFrame,
        view_name: str,
        database: str,
        columns: List[str]
    ) -> List[str]:
        """
        Split DataFrame and create multiple views.
        
        Args:
            df: DataFrame to split
            view_name: Base name for views
            database: Target database
            columns: List of column names
            
        Returns:
            List of created view names
        """
        view_names = []
        chunk_size = len(df) // 2  # Start by splitting in half

        # Keep splitting until all chunks fit
        while chunk_size > 0:
            view_names = []
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            all_fit = True
            for i, chunk in enumerate(chunks):
                chunk_view_name = f"{view_name}_part{i + 1}"
                sql = self._generate_values_sql(chunk, chunk_view_name, database, columns)
                sql_size = self.calculate_sql_size(sql)

                if sql_size > self.MAX_SQL_SIZE:
                    # This chunk is still too large, reduce chunk size
                    all_fit = False
                    chunk_size = chunk_size // 2
                    logger.info("Chunk still too large, reducing chunk size to %d", chunk_size)
                    break

            if all_fit:
                # All chunks fit, create the views
                for i, chunk in enumerate(chunks):
                    chunk_view_name = f"{view_name}_part{i + 1}"
                    # Drop existing view/table before creating
                    self._safe_drop_view_or_table(chunk_view_name, database)
                    sql = self._generate_values_sql(chunk, chunk_view_name, database, columns)
                    logger.info("Creating split view %s (%d rows)", chunk_view_name, len(chunk))
                    self._execute_view_creation(sql, database)
                    view_names.append(chunk_view_name)

                break

        return view_names
    
    def calculate_sql_size(self, sql: str) -> int:
        """
        Calculate SQL statement size in bytes.
        
        Args:
            sql: SQL statement string
            
        Returns:
            Size in bytes (UTF-8 encoding)
        """
        return len(sql.encode('utf-8'))
    
    def create_union_view(
        self,
        view_names: List[str],
        union_view_name: str,
        database: str
    ) -> bool:
        """
        Create a UNION view combining multiple split views.
        
        Args:
            view_names: List of view names to union
            union_view_name: Name for the combined view
            database: Target database name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Drop existing union view first
            self._safe_drop_view_or_table(union_view_name, database)

            # Build UNION ALL query
            union_parts = [f'SELECT * FROM {database}."{vn}"' for vn in view_names]
            union_query = '\nUNION ALL\n'.join(union_parts)

            sql = f"CREATE VIEW {database}.{union_view_name} AS\n{union_query}"

            logger.info("Creating UNION view from %d parts", len(view_names))
            self._execute_view_creation(sql, database)

            return True

        except Exception as e:
            logger.error("Failed to create UNION view: %s", str(e), exc_info=True)
            return False
    
    def _execute_view_creation(self, sql: str, database: str) -> None:
        """
        Execute view creation SQL using CID Athena helper.
        
        Args:
            sql: SQL statement to execute
            database: Database context for execution
            
        Raises:
            Exception: If query execution fails
        """
        try:
            # Execute the query using CID Athena helper
            self.athena_helper.query(
                sql=sql,
                database=database
            )
            logger.debug("Successfully executed view creation in %s", database)
            
        except Exception as e:
            logger.error("Failed to execute view creation: %s", str(e))
            raise


class S3Writer:
    """
    Writes account map DataFrames to S3 as Parquet files and creates Athena tables.
    
    The S3Writer handles writing DataFrames to S3 in Parquet format with snappy
    compression, and creates or updates Athena tables to query the data.
    
    Attributes:
        athena (Athena): CID Athena helper instance
        s3 (S3): CID S3 helper instance
    """
    
    def __init__(self, athena: Athena, s3):
        """
        Initialize S3Writer with Athena and S3 helpers.
        
        Args:
            athena: CID Athena helper instance
            s3: CID S3 helper instance
        """
        self.athena = athena
        self.s3 = s3
        self.s3_client = s3.client
        logger.info("S3Writer initialized")
    
    def write_parquet(
        self,
        df: pd.DataFrame,
        bucket: str,
        prefix: str
    ) -> str:
        """
        Write DataFrame to S3 as Parquet file with snappy compression.
        
        This method writes the DataFrame to S3 in Parquet format using pandas.
        The file is written with snappy compression for optimal storage and query
        performance.
        
        Args:
            df: DataFrame to write
            bucket: S3 bucket name
            prefix: S3 prefix/path (without trailing slash)
            
        Returns:
            Full S3 path (s3://bucket/prefix/file.parquet)
            
        Raises:
            Exception: If S3 write operation fails
            
        Example:
            >>> writer = S3Writer(config)
            >>> df = pd.DataFrame({'account_id': ['123456789012']})
            >>> s3_path = writer.write_parquet(df, 'my-bucket', 'account-maps')
            >>> print(s3_path)
            s3://my-bucket/account-maps/account_map.parquet
        """
        logger.info("Writing DataFrame to S3: s3://%s/%s", bucket, prefix)
        logger.info("DataFrame shape: %s", df.shape)
        
        try:
            # Ensure prefix doesn't have trailing slash
            prefix = prefix.rstrip('/')
            
            # Generate S3 key
            s3_key = f"{prefix}/account_map.parquet"
            s3_path = f"s3://{bucket}/{s3_key}"
            
            # Write DataFrame to Parquet in memory
            parquet_buffer = df.to_parquet(
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Upload to S3
            logger.info("Uploading Parquet file to S3: %s", s3_path)
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_buffer
            )
            
            logger.info("Successfully wrote Parquet file to %s", s3_path)
            return s3_path
            
        except Exception as e:
            logger.error("Failed to write Parquet to S3: %s", str(e), exc_info=True)
            raise
    
    def create_athena_table(
        self,
        s3_path: str,
        table_name: str,
        database: str,
        df: pd.DataFrame
    ) -> bool:
        """
        Create or update Athena table pointing to S3 Parquet file.
        
        This method creates an Athena external table that points to the Parquet
        file in S3. If the table already exists, it drops and recreates it to
        ensure the schema matches the current DataFrame.
        
        Args:
            s3_path: Full S3 path to Parquet file (s3://bucket/prefix/file.parquet)
            table_name: Name for the Athena table
            database: Target database name
            df: DataFrame used to generate table schema
            
        Returns:
            True if table creation was successful, False otherwise
            
        Example:
            >>> writer = S3Writer(config)
            >>> df = pd.DataFrame({'account_id': ['123456789012']})
            >>> writer.create_athena_table(
            ...     's3://my-bucket/account-maps/account_map.parquet',
            ...     'account_map',
            ...     'my_database',
            ...     df
            ... )
            True
        """
        logger.info("Creating Athena table: %s.%s", database, table_name)
        
        try:
            # Check if table exists and drop it
            self._drop_table_if_exists(table_name, database)
            
            # Generate and execute CREATE TABLE DDL
            ddl = self.generate_table_ddl(table_name, database, s3_path, df)
            logger.info("Executing CREATE TABLE statement")
            logger.debug("DDL: %s", ddl)
            
            self.athena_helper.query(
                sql=ddl,
                database=database
            )
            
            logger.info("Successfully created table: %s.%s", database, table_name)
            return True
            
        except Exception as e:
            logger.error("Failed to create Athena table: %s", str(e), exc_info=True)
            return False
    
    def generate_table_ddl(
        self,
        table_name: str,
        database: str,
        s3_path: str,
        df: pd.DataFrame
    ) -> str:
        """
        Generate CREATE EXTERNAL TABLE DDL from DataFrame schema.
        
        This method generates a complete DDL statement for creating an external
        table in Athena that points to a Parquet file in S3. The schema is
        derived from the DataFrame's column types.
        
        Args:
            table_name: Name for the table
            database: Target database
            s3_path: Full S3 path to Parquet file
            df: DataFrame to derive schema from
            
        Returns:
            Complete CREATE EXTERNAL TABLE DDL statement
        """
        # Extract S3 location (directory, not file)
        # s3://bucket/prefix/file.parquet -> s3://bucket/prefix/
        s3_location = '/'.join(s3_path.split('/')[:-1]) + '/'
        
        # Generate column definitions from DataFrame
        column_defs = []
        for col_name, dtype in df.dtypes.items():
            athena_type = self._pandas_to_athena_type(dtype)
            column_defs.append(f"  `{col_name}` {athena_type}")
        
        columns_clause = ',\n'.join(column_defs)
        
        # Build DDL statement
        ddl = f"""CREATE EXTERNAL TABLE {database}.{table_name} (
{columns_clause}
)
STORED AS PARQUET
LOCATION '{s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')"""
        
        return ddl
    
    def _pandas_to_athena_type(self, dtype) -> str:
        """
        Convert pandas dtype to Athena data type.
        
        Args:
            dtype: Pandas dtype object
            
        Returns:
            Athena data type as string
        """
        dtype_str = str(dtype)
        
        # Integer types
        if dtype_str.startswith('int'):
            return 'bigint'
        
        # Float types
        if dtype_str.startswith('float'):
            return 'double'
        
        # Boolean
        if dtype_str == 'bool':
            return 'boolean'
        
        # Datetime
        if dtype_str.startswith('datetime'):
            return 'timestamp'
        
        # Default to string for object and other types
        return 'string'
    
    def _drop_table_if_exists(self, table_name: str, database: str) -> None:
        """
        Drop table if it exists.
        
        Args:
            table_name: Name of table to drop
            database: Database containing the table
        """
        try:
            drop_sql = f"DROP TABLE IF EXISTS {database}.{table_name}"
            logger.info("Dropping existing table if present: %s.%s", database, table_name)
            
            self.athena_helper.query(
                sql=drop_sql,
                database=database
            )
            
            logger.debug("Successfully executed DROP TABLE IF EXISTS")
            
        except Exception as e:
            # Log but don't fail - table might not exist
            logger.warning("Error dropping table (may not exist): %s", str(e))

def extract_from_tag(
    org_data: pd.DataFrame,
    account_id: str,
    tag_key: str
) -> Optional[str]:
    """
    Extract value from hierarchytags by key.
    
    The hierarchytags column contains a list of dictionaries with 'key' and 'value'
    fields. This function searches for the specified tag_key and returns its value.
    
    Args:
        org_data: DataFrame containing organization data with hierarchytags column
        account_id: 12-digit account ID to look up
        tag_key: Tag key to search for in hierarchytags
        
    Returns:
        Tag value if found, None otherwise
        
    Example:
        >>> org_data = pd.DataFrame({
        ...     'id': ['123456789012'],
        ...     'hierarchytags': [[{'key': 'Environment', 'value': 'Production'}]]
        ... })
        >>> extract_from_tag(org_data, '123456789012', 'Environment')
        'Production'
    """
    try:
        # Find the row matching the account ID
        matching_rows = org_data.loc[org_data['id'] == account_id, 'hierarchytags']
        
        if matching_rows.empty:
            logger.warning("Account ID %s not found in organization data", account_id)
            return None
        
        # Get the hierarchytags list for this account
        tags_list = matching_rows.iloc[0]
        
        if not isinstance(tags_list, list):
            logger.warning("hierarchytags for account %s is not a list", account_id)
            return None
        
        # Search for the tag key
        for tag_item in tags_list:
            if isinstance(tag_item, dict) and tag_item.get('key') == tag_key:
                value = tag_item.get('value')
                logger.debug("Found tag %s=%s for account %s", tag_key, value, account_id)
                return value
        
        logger.debug("Tag key %s not found for account %s", tag_key, account_id)
        return None
        
    except Exception as e:
        logger.error(
            "Error extracting tag %s for account %s: %s",
            tag_key, account_id, str(e),
            exc_info=True
        )
        return None


def extract_from_account_name(
    org_data: pd.DataFrame,
    account_id: str,
    separator: str,
    index: int
) -> Optional[str]:
    """
    Split account name by separator and extract value at specified index.
    
    This function splits the account name using the provided separator and returns
    the part at the specified index position (0-based).
    
    Args:
        org_data: DataFrame containing organization data with name column
        account_id: 12-digit account ID to look up
        separator: Character(s) to split the account name by
        index: Zero-based index of the part to extract
        
    Returns:
        Extracted part of account name if found, None otherwise
        
    Example:
        >>> org_data = pd.DataFrame({
        ...     'id': ['123456789012'],
        ...     'name': ['dev-company-team-01']
        ... })
        >>> extract_from_account_name(org_data, '123456789012', '-', 1)
        'company'
    """
    try:
        # Find the row matching the account ID
        matching_rows = org_data.loc[org_data['id'] == account_id, 'name']
        
        if matching_rows.empty:
            logger.warning("Account ID %s not found in organization data", account_id)
            return None
        
        # Get the account name
        account_name = matching_rows.iloc[0]
        
        if not account_name or pd.isna(account_name):
            logger.warning("Account name is empty for account %s", account_id)
            return None
        
        # Split the account name
        parts = str(account_name).split(separator)
        
        # Check if index is valid
        if index < 0 or index >= len(parts):
            logger.warning(
                "Index %d out of range for account %s (name: %s, parts: %d)",
                index, account_id, account_name, len(parts)
            )
            return None
        
        value = parts[index]
        logger.debug(
            "Extracted '%s' from account name '%s' (separator: '%s', index: %d)",
            value, account_name, separator, index
        )
        return value
        
    except Exception as e:
        logger.error(
            "Error extracting from account name for account %s: %s",
            account_id, str(e),
            exc_info=True
        )
        return None


def extract_from_file(
    file_data: pd.DataFrame,
    account_id: str,
    column_name: str,
    account_id_column: str
) -> Optional[str]:
    """
    Extract value from external file by joining on account ID.
    
    This function looks up the account ID in the file data and returns the value
    from the specified column.
    
    Args:
        file_data: DataFrame containing external file data
        account_id: 12-digit account ID to look up
        column_name: Name of column to extract value from
        account_id_column: Name of column containing account IDs in file_data
        
    Returns:
        Value from specified column if found, None otherwise
        
    Example:
        >>> file_data = pd.DataFrame({
        ...     'AccountID': ['123456789012'],
        ...     'Department': ['Engineering']
        ... })
        >>> extract_from_file(file_data, '123456789012', 'Department', 'AccountID')
        'Engineering'
    """
    try:
        # Validate that required columns exist
        if account_id_column not in file_data.columns:
            logger.error(
                "Account ID column '%s' not found in file data. Available columns: %s",
                account_id_column, list(file_data.columns)
            )
            return None
        
        if column_name not in file_data.columns:
            logger.error(
                "Column '%s' not found in file data. Available columns: %s",
                column_name, list(file_data.columns)
            )
            return None
        
        # Find the row matching the account ID
        # Need to handle both string and numeric account IDs in the file
        matching_rows = file_data[file_data[account_id_column].astype(str).str.zfill(12) == account_id]
        
        if matching_rows.empty:
            logger.debug("Account ID %s not found in file data", account_id)
            return None
        
        # Get the value from the specified column
        value = matching_rows.iloc[0][column_name]
        
        if pd.isna(value):
            logger.debug("Value is null for account %s in column %s", account_id, column_name)
            return None
        
        logger.debug(
            "Extracted '%s' from file column '%s' for account %s",
            value, column_name, account_id
        )
        return str(value)
        
    except Exception as e:
        logger.error(
            "Error extracting from file for account %s: %s",
            account_id, str(e),
            exc_info=True
        )
        return None


def extract_payer_info(
    org_data: pd.DataFrame,
    account_id: str,
    info_type: str = 'name',
    payer_names: Optional[Dict[str, str]] = None
) -> Optional[str]:
    """
    Extract payer account information.
    
    This function retrieves the payer account ID or name for a given account.
    If custom payer names are provided, they will be used instead of looking up
    the payer account name from org_data.
    
    Args:
        org_data: DataFrame containing organization data with payer_id column
        account_id: 12-digit account ID to look up
        info_type: Type of information to extract ('id' or 'name')
        payer_names: Optional dictionary mapping payer IDs to custom names
        
    Returns:
        Payer account ID or name if found, None otherwise
        
    Example:
        >>> org_data = pd.DataFrame({
        ...     'id': ['123456789012'],
        ...     'payer_id': ['999888777666']
        ... })
        >>> extract_payer_info(org_data, '123456789012', 'id')
        '999888777666'
        >>> payer_names = {'999888777666': 'Production Payer'}
        >>> extract_payer_info(org_data, '123456789012', 'name', payer_names)
        'Production Payer'
    """
    try:
        # Find the row matching the account ID
        matching_rows = org_data.loc[org_data['id'] == account_id]
        
        if matching_rows.empty:
            logger.warning("Account ID %s not found in organization data", account_id)
            return None
        
        # Get the payer_id
        payer_id = matching_rows.iloc[0].get('payer_id')
        
        if not payer_id or pd.isna(payer_id):
            logger.debug("Payer ID is empty for account %s", account_id)
            return None
        
        # Ensure payer_id is 12 digits with leading zeros
        payer_id = str(payer_id).zfill(12)
        
        if info_type == 'id':
            logger.debug("Extracted payer ID %s for account %s", payer_id, account_id)
            return payer_id
        elif info_type == 'name':
            # Check if custom payer names are provided
            if payer_names and payer_id in payer_names:
                custom_name = payer_names[payer_id]
                logger.debug(
                    "Using custom payer name '%s' for payer ID %s (account %s)",
                    custom_name, payer_id, account_id
                )
                return custom_name
            
            # Fall back to looking up the payer account name from org_data
            payer_rows = org_data.loc[org_data['id'] == payer_id]
            if not payer_rows.empty:
                payer_name = payer_rows.iloc[0].get('name')
                if payer_name and not pd.isna(payer_name):
                    logger.debug(
                        "Extracted payer name '%s' for account %s",
                        payer_name, account_id
                    )
                    return str(payer_name)
            
            logger.debug("Payer name not found for payer ID %s", payer_id)
            return None
        else:
            logger.error("Invalid info_type: %s (must be 'id' or 'name')", info_type)
            return None
        
    except Exception as e:
        logger.error(
            "Error extracting payer info for account %s: %s",
            account_id, str(e),
            exc_info=True
        )
        return None


def _get_config_hash(config: dict) -> str:
    """
    Generate a hash of the Athena configuration to detect changes.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        String hash of relevant config values
    """
    athena_config = config.get('athena', {})
    general_config = config.get('general', {})
    
    # Create a string from relevant config values
    config_str = f"{general_config.get('aws_region')}|{athena_config.get('database')}|{athena_config.get('table')}"
    return config_str

def clear_athena_cache():
    """Clear the Athena data cache."""
    global _athena_data_cache
    _athena_data_cache['data'] = None
    _athena_data_cache['preview_data'] = None
    _athena_data_cache['columns'] = None
    _athena_data_cache['tag_keys'] = {}
    _athena_data_cache['config_hash'] = None
    logger.info("Cleared Athena data cache")

def _load_sample_accounts_for_preview(config: dict, athena: Athena = None) -> List[Dict[str, str]]:
    """
    Load a sample of 3 random accounts from Athena for name split preview.
    Uses LIMIT 10 query for efficiency.
    
    Args:
        config: Configuration dictionary containing Athena settings
        athena: Optional Athena helper instance from cid-cmd
        
    Returns:
        List of account dictionaries with 'id' and 'name', or empty list if loading fails
    """
    try:
        # Check if Athena is configured
        if 'athena' not in config or 'database' not in config['athena']:
            print("⚠️  Athena not configured yet, skipping preview\n")
            return []
        
        # Use cached preview data if available
        if _athena_data_cache['preview_data'] is not None:
            sample_data = _athena_data_cache['preview_data']
            print(f"✅ Using cached sample data ({len(sample_data)} accounts)\n")
        else:
            # Load fresh data with LIMIT 10 for efficiency
            print("\n🔍 Loading sample accounts from Athena for preview...")
            
            athena_config = config.get('athena', {})
            general_config = config.get('general', {})
            
            database = athena_config.get('database')
            table = athena_config.get('table')
            region = general_config.get('aws_region', 'us-east-1')
            workgroup = athena_config.get('workgroup', 'primary')
            
            # Create Athena helper if not provided
            if not athena:
                import boto3
                from cid.helpers import Athena
                session = boto3.Session(region_name=region)
                athena = Athena(session=session)
                athena._WorkGroup = workgroup
                catalog = athena_config.get('catalog', 'AwsDataCatalog')
                athena._CatalogName = catalog

            # Query for just 10 accounts (much faster than loading all)
            query = f'SELECT id, name FROM "{database}"."{table}" LIMIT 10'
            results = athena.query(sql=query, database=database, include_header=True)
            
            if not results or len(results) < 2:
                print("⚠️  No data returned from Athena, skipping preview\n")
                return []
            
            # Convert to list of dicts (skip header row)
            sample_data = []
            for row in results[1:]:  # Skip header
                if len(row) >= 2:
                    sample_data.append({
                        'id': str(row[0]),
                        'name': str(row[1])
                    })
            
            # Cache the preview data separately
            _athena_data_cache['preview_data'] = sample_data
            _athena_data_cache['config_hash'] = _get_config_hash(config)
            print(f"✅ Loaded {len(sample_data)} sample accounts\n")
        
        # Pick 3 random accounts from the sample
        import random
        sample_size = min(3, len(sample_data))
        accounts = random.sample(sample_data, sample_size)
        
        return accounts
        
    except Exception as e:
        print(f"⚠️  Could not load sample accounts: {str(e)}\n")
        logger.debug(f"Could not load sample accounts: {e}")
        return []

def _show_name_split_preview(sample_accounts: List[Dict[str, str]], separator: str):
    """
    Show how the separator splits the sample account names.
    
    Args:
        sample_accounts: List of account dicts with 'id' and 'name'
        separator: Separator character to use
    """
    print(f"\n📋 Preview: How '{separator}' splits your account names:")
    print("─" * 80)
    
    for account in sample_accounts:
        name = account['name']
        parts = name.split(separator)
        
        print(f"\n  Account: {name}")
        for i, part in enumerate(parts):
            print(f"    Index {i}: '{part}'")
    
    print("─" * 80)
    print()

def _select_index_with_preview(sample_accounts: List[Dict[str, str]], separator: str, level_name: str) -> int:
    """
    Interactively select index with live preview of results.
    Shows indices 0-4 and then offers custom option.
    
    Args:
        sample_accounts: List of account dicts with 'id' and 'name'
        separator: Separator character to use
        level_name: Name of the hierarchy level being configured
        
    Returns:
        Selected index (int)
    """
    if not sample_accounts:
        # Fallback to simple number input
        return int(inquirer.number(
            message="Index (0-based):",
            default=0,
            min_allowed=0
        ).execute())
    
    # Calculate max index from samples, but cap display at 4
    max_display_index = 4
    
    # Build choices with preview for indices 0-4
    choices = []
    for i in range(max_display_index + 1):
        # Show what this index would extract from each sample
        preview_values = []
        for account in sample_accounts:
            parts = account['name'].split(separator)
            if i < len(parts):
                preview_values.append(parts[i])
            else:
                preview_values.append('(empty)')
        
        # Truncate long values for display
        preview_values = [v[:20] + '...' if len(v) > 20 else v for v in preview_values]
        preview_str = ', '.join(f"'{v}'" for v in preview_values)
        choice_name = f"Index {i}: {preview_str}"
        choices.append(Choice(value=i, name=choice_name))
    
    # Add option for higher index
    choices.append(Choice(value='custom', name=f'Custom index (>4)'))
    
    selected = inquirer.select(
        message=f"Select index position for {level_name}:",
        choices=choices
    ).execute()
    
    if selected == 'custom':
        return int(inquirer.number(
            message="Index (0-based):",
            default=5,
            min_allowed=0
        ).execute())
    
    return selected

def get_file_columns(file_path: str) -> Optional[List[str]]:
    """
    Extract column names from a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        List of column names, or None if file cannot be read
    """
    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            return None
        
        suffix = file_path_obj.suffix.lower()
        
        if suffix == '.json':
            df = pd.read_json(file_path)
        elif suffix == '.csv':
            df = pd.read_csv(file_path, nrows=0)  # Just read headers
        elif suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path, nrows=0)  # Just read headers
        else:
            return None
        
        return df.columns.tolist()
    except Exception as e:
        logger.warning(f"Could not read file columns: {e}")
        return None

def get_available_tag_keys(config: dict, athena: Athena = None) -> Optional[List[str]]:
    """
    Get available tag keys from Athena data.
    
    Uses session-level caching to avoid reloading data multiple times.
    Prompts user to select which column contains tags, then extracts unique tag keys.
    
    Args:
        config: Configuration dictionary with Athena settings
        athena: Optional Athena helper instance from cid-cmd
        
    Returns:
        List of tag keys, or None if cannot be retrieved
    """
    global _athena_data_cache
    
    try:
        # Check if Athena is configured
        if not config.get('general', {}).get('aws_region'):
            print("⚠️  AWS region not configured")
            return None
        if not config.get('athena', {}).get('database'):
            print("⚠️  Athena database not configured")
            return None
        if not config.get('athena', {}).get('table'):
            print("⚠️  Athena table not configured")
            return None
        
        # Check if config has changed (invalidate cache if so)
        current_config_hash = _get_config_hash(config)
        if _athena_data_cache['config_hash'] != current_config_hash:
            logger.info("Athena configuration changed, clearing cache")
            clear_athena_cache()
            _athena_data_cache['config_hash'] = current_config_hash
        
        # Create Athena helper if not provided
        if not athena:
            import boto3
            from cid.helpers import Athena
            region = config['general']['aws_region']
            session = boto3.Session(region_name=region)
            athena = Athena(session=session)
            # Set workgroup and catalog from config to avoid interactive prompts
            workgroup = config.get('athena', {}).get('workgroup', 'primary')
            athena._WorkGroup = workgroup
            catalog = config.get('athena', {}).get('catalog', 'AwsDataCatalog')
            athena._CatalogName = catalog

        # Create data loader
        loader = DataLoader(athena, config)
        
        # Get available columns (use cache if available)
        if _athena_data_cache['columns'] is None:
            print("\n🔍 Fetching available columns from Athena...")
            try:
                columns = loader.get_available_columns()
                if not columns:
                    print("⚠️  No columns found in Athena table")
                    return None
                
                _athena_data_cache['columns'] = columns
                print(f"✅ Found {len(columns)} columns")
            except Exception as e:
                print(f"⚠️  Could not fetch columns: {e}")
                return None
        else:
            columns = _athena_data_cache['columns']
            print(f"\n✅ Using cached column list ({len(columns)} columns)")
        
        # Ask user which column contains tags
        print()
        tag_column = inquirer.select(
            message="Which column contains the tags?",
            choices=columns,
            default='hierarchytags' if 'hierarchytags' in columns else columns[0]
        ).execute()
        
        # Check if we already have tag keys for this column
        if tag_column in _athena_data_cache['tag_keys']:
            tag_keys = _athena_data_cache['tag_keys'][tag_column]
            print(f"\n✅ Using cached tag keys from {tag_column} ({len(tag_keys)} keys)")
            return tag_keys
        
        # Load data (use cache if available)
        if _athena_data_cache['data'] is None:
            print(f"\n🔍 Loading data from Athena to extract tag keys...")
            print("   (This may take a moment...)")
            
            try:
                org_data = loader.load_from_athena()
                
                if org_data.empty:
                    print("⚠️  No data returned from Athena")
                    return None
                
                _athena_data_cache['data'] = org_data
                print(f"✅ Loaded {len(org_data)} rows (cached for session)")
            except Exception as e:
                print(f"⚠️  Could not load data: {e}")
                return None
        else:
            org_data = _athena_data_cache['data']
            print(f"\n✅ Using cached data ({len(org_data)} rows)")
        
        # Extract tag keys from the selected column
        print(f"🔍 Extracting tag keys from {tag_column} column...")
        try:
            tag_keys = loader.get_available_tag_keys(org_data, tag_column)
            
            if tag_keys:
                # Cache the tag keys for this column
                _athena_data_cache['tag_keys'][tag_column] = tag_keys
                print(f"✅ Found {len(tag_keys)} unique tag keys")
                return tag_keys
            else:
                print("⚠️  No tag keys found")
                return None
                
        except Exception as e:
            print(f"⚠️  Could not extract tag keys: {e}")
            return None
            
    except Exception as e:
        logger.warning(f"Could not retrieve tag keys from Athena: {e}")
        print(f"⚠️  Error: {e}")
        return None

def show_main_menu() -> str:
    """
    Display main configuration menu.
    
    Args:
        config: Current configuration dictionary
        
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

def configure_athena_settings(config: dict) -> dict:
    """
    Configure Athena settings section.
    
    Args:
        config: Current configuration dictionary
        
    Returns:
        Updated configuration dictionary
    """
    if 'athena' not in config:
        config['athena'] = {}
    
    print("\n🗄️  Athena Configuration\n")
    
    # Catalog
    config['athena']['catalog'] = inquirer.text(
        message="Athena Catalog:",
        default=config['athena'].get('catalog', 'AwsDataCatalog')
    ).execute()
    
    # Workgroup
    config['athena']['workgroup'] = inquirer.text(
        message="Athena Workgroup:",
        default=config['athena'].get('workgroup', 'primary')
    ).execute()
    
    # Database
    config['athena']['database'] = inquirer.text(
        message="Source Database:",
        default=config['athena'].get('database', '')
    ).execute()
    
    # Table
    config['athena']['table'] = inquirer.text(
        message="Source Table:",
        default=config['athena'].get('table', '')
    ).execute()
    
    # Target Database
    config['athena']['database_target'] = inquirer.text(
        message="Target Database (leave empty to use source database):",
        default=config['athena'].get('database_target', config['athena'].get('database', ''))
    ).execute()
    
    # Output Mode
    config['athena']['output_mode'] = inquirer.select(
        message="Output Mode:",
        choices=[
            Choice(value="view", name="Athena View"),
            Choice(value="parquet", name="S3 Parquet with Athena Table")
        ],
        default=config['athena'].get('output_mode', 'view')
    ).execute()
    
    # Output Table Name
    config['athena']['output_table_name'] = inquirer.text(
        message="Output Table/View Name:",
        default=config['athena'].get('output_table_name', 'account_map')
    ).execute()
    
    print("\n✅ Athena settings updated\n")
    return config

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

def configure_rules(config: dict) -> dict:
    """
    Configure business rules section.
    
    Args:
        config: Current configuration dictionary
        
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
            config = add_hierarchy_level(config)
        elif action == "modify":
            config = modify_hierarchy_level(config)
        elif action == "remove":
            config = remove_hierarchy_level(config)
        elif action == "done":
            break
    
    print("\n✅ Business rules updated\n")
    return config

def add_hierarchy_level(config: dict) -> dict:
    """
    Add a new hierarchy level.
    
    Args:
        config: Current configuration dictionary
        
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
        tag_keys = get_available_tag_keys(config)
        
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
        sample_accounts = _load_sample_accounts_for_preview(config)
        
        parameters['separator'] = inquirer.text(
            message="Separator Character:",
            default="-"
        ).execute()
        
        # Show preview with the selected separator
        if sample_accounts:
            print(f"\n💡 Showing live preview with your actual account data:")
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
        # Ask if user wants to use payer ID or assign custom names
        use_custom_names = inquirer.confirm(
            message="Would you like to assign custom names to payer IDs?",
            default=False
        ).execute()
        
        parameters['use_custom_names'] = use_custom_names
        
        if use_custom_names:
            # Get unique payer IDs from Athena data
            print("\n🔍 Fetching unique payer IDs from Athena...")
            try:
                from data.loader import DataLoader
                
                # Check if we have cached data
                if _athena_data_cache['data'] is not None:
                    org_data = _athena_data_cache['data']
                    print(f"✅ Using cached data ({len(org_data)} rows)")
                else:
                    # Load data
                    loader = DataLoader(config)
                    org_data = loader.load_from_athena()
                    print(f"✅ Loaded {len(org_data)} rows")
                
                # Get unique payer IDs
                if 'payer_id' in org_data.columns:
                    unique_payers = org_data['payer_id'].dropna().unique()
                    unique_payers = sorted([str(p).zfill(12) for p in unique_payers])
                    
                    print(f"✅ Found {len(unique_payers)} unique payer IDs\n")
                    
                    # Ask user to name each payer
                    payer_names = {}
                    for payer_id in unique_payers:
                        # Try to get the account name for this payer from org_data
                        payer_rows = org_data[org_data['id'] == payer_id]
                        default_name = payer_rows.iloc[0]['name'] if not payer_rows.empty else payer_id
                        
                        payer_name = inquirer.text(
                            message=f"Name for payer ID {payer_id}:",
                            default=default_name
                        ).execute()
                        
                        payer_names[payer_id] = payer_name
                    
                    parameters['payer_names'] = payer_names
                    print(f"\n✅ Configured names for {len(payer_names)} payers\n")
                else:
                    print("⚠️  payer_id column not found in data")
                    parameters['use_custom_names'] = False
                    
            except Exception as e:
                print(f"⚠️  Could not fetch payer IDs: {e}")
                parameters['use_custom_names'] = False
    
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

def modify_hierarchy_level(config: dict) -> dict:
    """
    Modify an existing hierarchy level.
    
    Args:
        config: Current configuration dictionary
        
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
    
    # Remove the old level
    old_level = config['rules']['hierarchy_levels'].pop(idx)
    
    # Add it back with modifications (reuse add logic)
    print(f"\nModifying: Level {old_level['level']}: {old_level['name']}\n")
    
    # Level number
    level_num = inquirer.number(
        message="Level Number:",
        default=old_level['level'],
        min_allowed=1
    ).execute()
    
    # Level name
    level_name = inquirer.text(
        message="Level Name:",
        default=old_level['name']
    ).execute()
    
    # Source type
    source = inquirer.select(
        message="Data Source:",
        choices=[
            Choice(value="athena_tags", name="Athena Tags (hierarchytags)"),
            Choice(value="athena_name", name="Account Name (split by separator)"),
            Choice(value="athena_payer", name="Payer Account Information"),
            Choice(value="file", name="External File")
        ],
        default=old_level['source']
    ).execute()
    
    # Get parameters based on source type
    parameters = {}
    old_params = old_level.get('parameters', {})
    
    if source == "athena_tags":
        # Try to get available tag keys from Athena
        tag_keys = get_available_tag_keys(config)
        
        if tag_keys:
            print(f"\n✅ Found {len(tag_keys)} tag keys in Athena data\n")
            default_tag = old_params.get('tag_key', '') if old_params.get('tag_key', '') in tag_keys else tag_keys[0]
            parameters['tag_key'] = inquirer.select(
                message="Select Tag Key:",
                choices=tag_keys,
                default=default_tag
            ).execute()
        else:
            print("\n⚠️  Could not retrieve tag keys from Athena, using manual input\n")
            parameters['tag_key'] = inquirer.text(
                message="Tag Key:",
                default=old_params.get('tag_key', '')
            ).execute()
    
    elif source == "athena_name":
        # Load sample accounts for preview
        sample_accounts = _load_sample_accounts_for_preview(config)
        
        parameters['separator'] = inquirer.text(
            message="Separator Character:",
            default=old_params.get('separator', '-')
        ).execute()
        
        # Show preview with the selected separator
        if sample_accounts:
            print(f"\n💡 Showing live preview with your actual account data:")
            _show_name_split_preview(sample_accounts, parameters['separator'])
            parameters['index'] = _select_index_with_preview(sample_accounts, parameters['separator'], level_name)
        else:
            print("\n⚠️  Preview not available - using manual input")
            parameters['index'] = int(inquirer.number(
                message="Index (0-based):",
                default=old_params.get('index', 0),
                min_allowed=0
            ).execute())
    
    elif source == "athena_payer":
        # Ask if user wants to use payer ID or assign custom names
        use_custom_names = inquirer.confirm(
            message="Would you like to assign custom names to payer IDs?",
            default=old_params.get('use_custom_names', False)
        ).execute()
        
        parameters['use_custom_names'] = use_custom_names
        
        if use_custom_names:
            # Reuse existing payer names if available
            parameters['payer_names'] = old_params.get('payer_names', {})
            
            # Ask if user wants to update the payer names
            update_names = inquirer.confirm(
                message=f"Update payer names? (Currently {len(parameters['payer_names'])} configured)",
                default=False
            ).execute()
            
            if update_names or not parameters['payer_names']:
                # Get unique payer IDs from Athena data
                print("\n🔍 Fetching unique payer IDs from Athena...")
                try:
                    from data.loader import DataLoader
                    
                    # Check if we have cached data
                    if _athena_data_cache['data'] is not None:
                        org_data = _athena_data_cache['data']
                        print(f"✅ Using cached data ({len(org_data)} rows)")
                    else:
                        # Load data
                        loader = DataLoader(config)
                        org_data = loader.load_from_athena()
                        print(f"✅ Loaded {len(org_data)} rows")
                    
                    # Get unique payer IDs
                    if 'payer_id' in org_data.columns:
                        unique_payers = org_data['payer_id'].dropna().unique()
                        unique_payers = sorted([str(p).zfill(12) for p in unique_payers])
                        
                        print(f"✅ Found {len(unique_payers)} unique payer IDs\n")
                        
                        # Ask user to name each payer
                        payer_names = {}
                        for payer_id in unique_payers:
                            # Try to get existing name or default
                            existing_name = parameters['payer_names'].get(payer_id)
                            if not existing_name:
                                payer_rows = org_data[org_data['id'] == payer_id]
                                existing_name = payer_rows.iloc[0]['name'] if not payer_rows.empty else payer_id
                            
                            payer_name = inquirer.text(
                                message=f"Name for payer ID {payer_id}:",
                                default=existing_name
                            ).execute()
                            
                            payer_names[payer_id] = payer_name
                        
                        parameters['payer_names'] = payer_names
                        print(f"\n✅ Configured names for {len(payer_names)} payers\n")
                    else:
                        print("⚠️  payer_id column not found in data")
                        parameters['use_custom_names'] = False
                        
                except Exception as e:
                    print(f"⚠️  Could not fetch payer IDs: {e}")
                    parameters['use_custom_names'] = False
    
    elif source == "file":
        # Try to get columns from configured file
        file_path = config.get('file_source', {}).get('file_path')
        file_columns = get_file_columns(file_path) if file_path else None
        
        if file_columns:
            print(f"\n✅ Found {len(file_columns)} columns in file\n")
            
            default_col = old_params.get('column_name', '') if old_params.get('column_name', '') in file_columns else file_columns[0]
            parameters['column_name'] = inquirer.select(
                message="Select Column for Hierarchy Level:",
                choices=file_columns,
                default=default_col
            ).execute()
            
            default_id_col = old_params.get('account_id_column', 'account_id')
            if default_id_col not in file_columns:
                default_id_col = file_columns[0]
            parameters['account_id_column'] = inquirer.select(
                message="Select Account ID Column:",
                choices=file_columns,
                default=default_id_col
            ).execute()
        else:
            if file_path:
                print("\n⚠️  Could not read file columns, using manual input\n")
            else:
                print("\n⚠️  No file configured yet, using manual input\n")
            
            parameters['column_name'] = inquirer.text(
                message="Column Name in File:",
                default=old_params.get('column_name', '')
            ).execute()
            
            parameters['account_id_column'] = inquirer.text(
                message="Account ID Column in File:",
                default=old_params.get('account_id_column', 'account_id')
            ).execute()
    
    # Create modified level
    modified_level = {
        'level': int(level_num),
        'name': level_name,
        'source': source,
        'parameters': parameters
    }
    
    config['rules']['hierarchy_levels'].append(modified_level)
    
    # Sort by level number
    config['rules']['hierarchy_levels'].sort(key=lambda x: x['level'])
    
    print(f"\n✅ Modified level {level_num}: {level_name}\n")
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
    
    # Check if output mode is parquet
    if config.get('athena', {}).get('output_mode') == 'parquet':
        print("ℹ️  S3 output is required when output mode is 'parquet'\n")
        config['s3_output']['enabled'] = True
    else:
        # Enable S3 output
        config['s3_output']['enabled'] = inquirer.confirm(
            message="Enable S3 Parquet output?",
            default=config['s3_output'].get('enabled', False)
        ).execute()
    
    if config['s3_output']['enabled']:
        # Bucket
        config['s3_output']['bucket'] = inquirer.text(
            message="S3 Bucket:",
            default=config['s3_output'].get('bucket', '')
        ).execute()
        
        # Prefix
        config['s3_output']['prefix'] = inquirer.text(
            message="S3 Prefix/Path:",
            default=config['s3_output'].get('prefix', 'account-map/')
        ).execute()
        
        # Table name
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
        print(f"  AWS Region: {config['general'].get('aws_region', 'Not set')}")
        print(f"  Log Directory: {config['general'].get('log_directory', 'Not set')}")
        print()
    
    # Athena Configuration
    if 'athena' in config:
        print("🗄️  Athena Configuration:")
        print(f"  Catalog: {config['athena'].get('catalog', 'Not set')}")
        print(f"  Workgroup: {config['athena'].get('workgroup', 'Not set')}")
        print(f"  Source Database: {config['athena'].get('database', 'Not set')}")
        print(f"  Source Table: {config['athena'].get('table', 'Not set')}")
        print(f"  Target Database: {config['athena'].get('database_target', 'Not set')}")
        print(f"  Output Mode: {config['athena'].get('output_mode', 'Not set')}")
        print(f"  Output Table/View: {config['athena'].get('output_table_name', 'Not set')}")
        print()
    
    # File Source
    if 'file_source' in config and config['file_source'].get('enabled'):
        print("📁 File Source Configuration:")
        print(f"  Enabled: {config['file_source'].get('enabled', False)}")
        print(f"  File Path: {config['file_source'].get('file_path', 'Not set')}")
        print(f"  Account ID Column: {config['file_source'].get('account_id_column', 'Not set')}")
        print(f"  Account Name Column: {config['file_source'].get('account_name_column', 'Not set')}")
        print(f"  Payer Name Column: {config['file_source'].get('payer_name_column', 'Not set')}")
        print()
    
    # Business Rules
    if 'rules' in config and config['rules'].get('hierarchy_levels'):
        print("📊 Business Rules Configuration:")
        print(f"  Hierarchy Levels: {len(config['rules']['hierarchy_levels'])}")
        for level in config['rules']['hierarchy_levels']:
            print(f"    Level {level['level']}: {level['name']}")
            print(f"      Source: {level['source']}")
            if level.get('parameters'):
                for key, value in level['parameters'].items():
                    print(f"      {key}: {value}")
        print()
    
    # S3 Output
    if 's3_output' in config and config['s3_output'].get('enabled'):
        print("☁️  S3 Output Configuration:")
        print(f"  Enabled: {config['s3_output'].get('enabled', False)}")
        print(f"  Bucket: {config['s3_output'].get('bucket', 'Not set')}")
        print(f"  Prefix: {config['s3_output'].get('prefix', 'Not set')}")
        print(f"  Table Name: {config['s3_output'].get('table_name', 'Not set')}")
        print()
    
    print("="*60 + "\n")
    
    # Wait for user to continue
    inquirer.text(
        message="Press Enter to continue...",
        default=""
    ).execute()


logger = logging.getLogger(__name__)

def validate_config(config: dict) -> Tuple[bool, List[str]]:
    """
    Validate complete configuration.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # Validate each section
    errors.extend(validate_general_config(config.get('general', {})))
    errors.extend(validate_athena_config(config.get('athena', {})))
    errors.extend(validate_file_source_config(config.get('file_source', {})))
    errors.extend(validate_rules_config(config.get('rules', {})))
    errors.extend(validate_s3_output_config(config.get('s3_output', {})))
    
    is_valid = len(errors) == 0
    
    if is_valid:
        logger.info("Configuration validation passed")
    else:
        logger.warning(f"Configuration validation failed with {len(errors)} error(s)")
    
    return is_valid, errors

def validate_general_config(general_config: dict) -> List[str]:
    """
    Validate general configuration section.
    
    Args:
        general_config: General configuration dictionary
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Required fields
    if 'aws_region' not in general_config or not general_config['aws_region']:
        errors.append("general.aws_region is required")
    
    # Optional fields with defaults
    if 'log_directory' in general_config and not isinstance(general_config['log_directory'], str):
        errors.append("general.log_directory must be a string")
    
    return errors

def validate_athena_config(athena_config: dict) -> List[str]:
    """
    Validate Athena configuration section.
    
    Args:
        athena_config: Athena configuration dictionary
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Required fields
    required_fields = ['catalog', 'workgroup', 'database', 'table', 'output_mode', 'output_table_name']
    for field in required_fields:
        if field not in athena_config or not athena_config[field]:
            errors.append(f"athena.{field} is required")
    
    # Validate output_mode
    if 'output_mode' in athena_config:
        valid_modes = ['view', 'parquet']
        if athena_config['output_mode'] not in valid_modes:
            errors.append(f"athena.output_mode must be one of {valid_modes}")
    
    # database_target is optional, defaults to database
    
    return errors

def validate_file_source_config(file_config: dict) -> List[str]:
    """
    Validate file source configuration section.
    
    Args:
        file_config: File source configuration dictionary
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # File source is optional, but if enabled, must have required fields
    if file_config.get('enabled', False):
        required_fields = ['file_path', 'account_id_column', 'account_name_column']
        for field in required_fields:
            if field not in file_config or not file_config[field]:
                errors.append(f"file_source.{field} is required when file_source is enabled")
    
    return errors

def validate_rules_config(rules_config: dict) -> List[str]:
    """
    Validate rules configuration section.
    
    Args:
        rules_config: Rules configuration dictionary
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # hierarchy_levels is required
    if 'hierarchy_levels' not in rules_config:
        errors.append("rules.hierarchy_levels is required")
        return errors
    
    hierarchy_levels = rules_config['hierarchy_levels']
    
    if not isinstance(hierarchy_levels, list):
        errors.append("rules.hierarchy_levels must be a list")
        return errors
    
    if len(hierarchy_levels) == 0:
        errors.append("rules.hierarchy_levels must contain at least one level")
        return errors
    
    # Validate each hierarchy level
    for idx, level in enumerate(hierarchy_levels):
        level_errors = validate_hierarchy_level(level, idx)
        errors.extend(level_errors)
    
    return errors

def validate_hierarchy_level(level: dict, index: int) -> List[str]:
    """
    Validate a single hierarchy level configuration.
    
    Args:
        level: Hierarchy level configuration dictionary
        index: Index of the level in the list (for error messages)
        
    Returns:
        List of validation errors
    """
    errors = []
    prefix = f"rules.hierarchy_levels[{index}]"
    
    # Required fields
    if 'level' not in level:
        errors.append(f"{prefix}.level is required")
    elif not isinstance(level['level'], int):
        errors.append(f"{prefix}.level must be an integer")
    
    if 'name' not in level or not level['name']:
        errors.append(f"{prefix}.name is required")
    
    if 'source' not in level or not level['source']:
        errors.append(f"{prefix}.source is required")
    else:
        # Validate source type and parameters
        source = level['source']
        valid_sources = ['athena_tags', 'athena_name', 'athena_payer', 'file']
        
        if source not in valid_sources:
            errors.append(f"{prefix}.source must be one of {valid_sources}")
        else:
            # Validate parameters based on source type
            params = level.get('parameters', {})
            
            if source == 'athena_tags':
                if 'tag_key' not in params or not params['tag_key']:
                    errors.append(f"{prefix}.parameters.tag_key is required for athena_tags source")
            
            elif source == 'athena_name':
                if 'separator' not in params or not params['separator']:
                    errors.append(f"{prefix}.parameters.separator is required for athena_name source")
                if 'index' not in params:
                    errors.append(f"{prefix}.parameters.index is required for athena_name source")
                elif not isinstance(params['index'], int):
                    errors.append(f"{prefix}.parameters.index must be an integer")
            
            elif source == 'file':
                if 'column_name' not in params or not params['column_name']:
                    errors.append(f"{prefix}.parameters.column_name is required for file source")
                if 'account_id_column' not in params or not params['account_id_column']:
                    errors.append(f"{prefix}.parameters.account_id_column is required for file source")
            
            # athena_payer doesn't require additional parameters
    
    return errors

def validate_s3_output_config(s3_config: dict) -> List[str]:
    """
    Validate S3 output configuration section.
    
    Args:
        s3_config: S3 output configuration dictionary
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # S3 output is optional, but if enabled, must have required fields
    if s3_config.get('enabled', False):
        required_fields = ['bucket', 'prefix', 'table_name']
        for field in required_fields:
            if field not in s3_config or not s3_config[field]:
                errors.append(f"s3_output.{field} is required when s3_output is enabled")
    
    return errors
