import json
import logging
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import pandas as pd
import boto3

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.separator import Separator

from cid.helpers import Athena
from cid.exceptions import CidCritical

logger = logging.getLogger(__name__)


@contextmanager
def spinner(message: str = "Processing..."):
    """Context manager that shows a spinning indicator while work is in progress."""
    stop_event = threading.Event()
    frames = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

    def _spin():
        i = 0
        while not stop_event.is_set():
            sys.stdout.write(f"\r{frames[i % len(frames)]} {message}")
            sys.stdout.flush()
            i += 1
            time.sleep(0.1)
        sys.stdout.write(f"\r✅ {message} done\n")
        sys.stdout.flush()

    t = threading.Thread(target=_spin, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop_event.set()
        t.join()


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
    Represents an AWS account with metadata and business taxonomy information.
    
    The Account class ensures that account IDs are properly formatted (12 digits with
    leading zeros) and provides class-level storage for all account instances to enable
    easy lookup and export.
    
    Attributes:
        _account_id (str): 12-digit account ID
        _account_name (str): Account name
        _payer_account_id (str): Payer account ID (12 digits)
        _payer_account_name (str): Payer account name
        _account_tags (dict): Account tags
        _business_unit (dict): Business unit taxonomy information
        
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
        Set a business unit taxonomy dimension for this account.
        
        Args:
            bu_name: Business unit dimension name (e.g., 'level_1', 'level_2')
            bu_value: Business unit value
        """
        self._business_unit[bu_name] = bu_value
        Account._all_accounts[self._account_id][bu_name] = bu_value
    
    def get_business_unit(self) -> Dict[str, str]:
        """
        Get all business unit information for this account.
        
        Returns:
            Dictionary of business unit dimensions and values
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


class TransformEngine:
    """
    Orchestrates data transformation according to configured rules.
    
    The TransformEngine applies taxonomy rules to organization data, creating
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
        4. Apply all taxonomy rules
        5. Convert to DataFrame
        
        Returns:
            DataFrame containing account map with all taxonomy dimensions
            
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
                    # Note: payer_name is only set if used in a taxonomy rule
                
                account_count += 1
                
            except Exception as e:
                logger.error(
                    "Error creating account %s: %s",
                    account_id, str(e),
                    exc_info=True
                )
                continue
        
        logger.info("Created %d Account instances", account_count)
        
        # Apply taxonomy rules
        self.apply_taxonomy_rules()
        
        # Convert to DataFrame
        result_df = Account.to_dataframe()
        logger.info("Transformation complete. Generated %d rows", len(result_df))
        
        return result_df
    
    def apply_taxonomy_rules(self) -> None:
        """
        Apply all configured taxonomy dimension rules.
        
        This method iterates through all configured taxonomy dimensions and applies
        the appropriate extraction rule for each account.
        """
        taxonomy_dimensions = self.config.get('taxonomy_dimensions', [])
        
        if not taxonomy_dimensions:
            logger.warning("No taxonomy dimensions configured")
            return
        
        logger.info("Applying %d taxonomy rules", len(taxonomy_dimensions))
        
        # Sort dimensions by level number to ensure correct order (if level exists)
        sorted_dimensions = sorted(taxonomy_dimensions, key=lambda x: x.get('level', 0))
        
        # Get all accounts
        all_accounts = Account.get_all_accounts()
        
        # Apply each rule to each account
        for dimension_config in sorted_dimensions:
            dimension_num = dimension_config.get('level')
            dimension_name = dimension_config.get('name', f'level_{dimension_num}')
            
            logger.debug("Applying rule for %s", dimension_name)
            
            success_count = 0
            for account_id in all_accounts.keys():
                try:
                    # Apply the rule for this dimension
                    value = self.apply_single_rule(dimension_config, account_id)
                    
                    if value is not None:
                        # Create Account instance to set the business unit
                        account = Account(account_id)
                        account.set_business_unit(dimension_name, value)
                        success_count += 1
                    
                except Exception as e:
                    logger.error(
                        "Error applying rule %s to account %s: %s",
                        dimension_name, account_id, str(e),
                        exc_info=True
                    )
            
            logger.info(
                "Applied rule %s: %d/%d accounts successful",
                dimension_name, success_count, len(all_accounts)
            )
    
    def apply_single_rule(self, rule: Dict[str, Any], account_id: str) -> Optional[str]:
        """
        Apply a single transformation rule to an account.
        
        This method determines the rule source type and calls the appropriate
        extraction function with the configured parameters.
        
        Config structure: {'name': 'BU', 'source_type': 'tag', 'source_value': 'BU'}
        
        Args:
            rule: Rule configuration dictionary containing source_type and source_value
            account_id: 12-digit account ID to apply rule to
            
        Returns:
            Extracted value if successful, None otherwise
            
        Raises:
            ValueError: If rule source type is unknown or required parameters are missing
        """
        # Support new config structure
        source_type = rule.get('source_type')
        source_value = rule.get('source_value')
        
        if source_type:
            # New config structure
            try:
                if source_type == 'tag':
                    return extract_from_tag(self.org_data, account_id, source_value)
                
                elif source_type == 'file':
                    if self.file_data is None:
                        logger.error("File source specified but no file data loaded")
                        return None
                    
                    # In new structure, source_value is the column name in the file
                    # and we need to get the account_id_column from config
                    account_id_column = self.config.get('file_source', {}).get('account_column', 'account_id')
                    return extract_from_file(
                        self.file_data,
                        account_id,
                        source_value,
                        account_id_column
                    )
                
                elif source_type == 'name_split':
                    # Extract from account name by splitting
                    separator = source_value.get('separator')
                    index = source_value.get('index')
                    
                    if separator is None or index is None:
                        logger.error("name_split source requires 'separator' and 'index' in source_value")
                        return None
                    
                    return extract_from_account_name(
                        self.org_data,
                        account_id,
                        separator,
                        int(index)
                    )
                
                else:
                    logger.error("Unknown source_type: %s", source_type)
                    return None
                    
            except Exception as e:
                logger.error(
                    "Error applying rule with source_type %s to account %s: %s",
                    source_type, account_id, str(e),
                    exc_info=True
                )
                return None
        
        # No valid source_type found
        logger.error("Rule missing 'source_type' field: %s", rule)
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
                    managementaccountid AS payer_id,
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
    
    def load_from_file(self, file_path: Optional[str] = None, account_id_column: Optional[str] = None, 
                       validate_ids: bool = False) -> pd.DataFrame:
        """
        Load data from file (Excel, CSV, or JSON) with optional account ID validation.
        
        Args:
            file_path: Path to file. If None, uses path from configuration.
            account_id_column: Name of column containing account IDs for validation.
                              If None and validate_ids is True, will attempt auto-detection.
            validate_ids: If True, validate account IDs using Account class.
            
        Returns:
            DataFrame containing file data
            
        Raises:
            ValueError: If file path is not provided or configured
            FileNotFoundError: If file does not exist
            ValueError: If file format is not supported
            ValueError: If validate_ids is True and account IDs are invalid
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
            
            # Validate account IDs if requested
            if validate_ids:
                # Determine account ID column
                if account_id_column is None:
                    # Try to auto-detect
                    account_id_column = self.auto_detect_account_column(df)
                    if account_id_column is None:
                        raise ValueError(
                            "Could not auto-detect account ID column. "
                            "Please specify account_id_column parameter."
                        )
                    logger.info("Auto-detected account ID column: %s", account_id_column)
                
                # Validate the account IDs
                is_valid, invalid_ids = self.validate_account_ids(df, account_id_column)
                
                if not is_valid:
                    error_msg = (
                        f"Found {len(invalid_ids)} invalid account IDs in column '{account_id_column}':\n"
                        + "\n".join(invalid_ids[:10])  # Show first 10 invalid IDs
                    )
                    if len(invalid_ids) > 10:
                        error_msg += f"\n... and {len(invalid_ids) - 10} more"
                    
                    logger.error("Account ID validation failed: %s", error_msg)
                    raise ValueError(error_msg)
                
                logger.info("Account ID validation passed for column '%s'", account_id_column)
            
            return df
            
        except Exception as e:
            logger.error("Failed to load file %s: %s", file_path, str(e), exc_info=True)
            raise
    
    def auto_detect_account_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Auto-detect the account ID column in a DataFrame.
        
        Searches for common account ID column name patterns (case-insensitive):
        - account_id
        - accountid
        - account
        - id
        - aws_account_id
        
        Args:
            df: DataFrame to search
            
        Returns:
            Column name if found, None otherwise
            
        Example:
            >>> df = pd.DataFrame({'Account_ID': ['123456789012'], 'Name': ['Test']})
            >>> loader = DataLoader(athena, {})
            >>> loader.auto_detect_account_column(df)
            'Account_ID'
        """
        # Common account ID column name patterns (in priority order)
        patterns = ['account_id', 'accountid', 'account', 'aws_account_id', 'id']
        
        # Convert all column names to lowercase for comparison
        columns_lower = {col.lower(): col for col in df.columns}
        
        # Search for patterns in priority order
        for pattern in patterns:
            if pattern in columns_lower:
                detected_col = columns_lower[pattern]
                logger.debug("Auto-detected account ID column: %s (matched pattern: %s)", 
                           detected_col, pattern)
                return detected_col
        
        logger.debug("Could not auto-detect account ID column from available columns: %s", 
                    list(df.columns))
        return None
    
    def validate_account_ids(self, df: pd.DataFrame, account_column: str) -> Tuple[bool, List[str]]:
        """
        Validate that all account IDs in the specified column are valid 12-digit AWS account IDs.

        Uses the Account class to validate each account ID, ensuring they consist of
        12 digits (0-9) and can be properly zero-padded.

        Args:
            df: DataFrame containing account data
            account_column: Name of the column containing account IDs

        Returns:
            Tuple of (is_valid, invalid_ids) where:
                - is_valid: True if all account IDs are valid, False otherwise
                - invalid_ids: List of invalid account ID values

        Example:
            >>> df = pd.DataFrame({'account_id': ['123456789012', 'invalid', '999']})
            >>> loader = DataLoader(athena, {})
            >>> is_valid, invalid = loader.validate_account_ids(df, 'account_id')
            >>> print(is_valid)
            False
            >>> print(invalid)
            ['invalid']
        """
        if account_column not in df.columns:
            raise ValueError(f"Column '{account_column}' not found in DataFrame")

        invalid_ids = []

        for idx, value in df[account_column].items():
            # Skip NaN/None values
            if pd.isna(value):
                invalid_ids.append(f"Row {idx}: <empty>")
                continue

            # Try to create an Account instance to validate the ID
            try:
                Account(str(value))
            except (TypeError, ValueError) as e:
                invalid_ids.append(f"Row {idx}: {value}")
                logger.debug("Invalid account ID at row %d: %s - %s", idx, value, str(e))

        is_valid = len(invalid_ids) == 0

        if is_valid:
            logger.info("All %d account IDs in column '%s' are valid", len(df), account_column)
        else:
            logger.warning("Found %d invalid account IDs in column '%s'", len(invalid_ids), account_column)

        return is_valid, invalid_ids
    
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
    """
    Manages account mapper configuration storage and retrieval from Athena views.
    
    The ConfigManager handles reading and writing configuration to Athena views
    instead of files. Configuration is stored in a view named {view_name}_config
    using a two-row-type structure with metadata and dimension rows.
    
    Attributes:
        athena (Athena): CID Athena helper instance
        view_name (str): Base view name (default: "account_map")
    """
    
    def __init__(self, athena: Athena, view_name: str = "account_map"):
        """
        Initialize configuration manager with Athena helper and view name.
        
        Args:
            athena: CID Athena helper instance
            view_name: Base view name for configuration (default: "account_map")
        """
        self.athena = athena
        self.view_name = view_name
        self.config_view_name = f"{view_name}_config"
        logger.info("ConfigManager initialized for view: %s", self.config_view_name)
    
    def load_from_view(self, database: str) -> Optional[dict]:
        """
        Load configuration from Athena config view.
        
        Args:
            database: Database containing the config view
            
        Returns:
            Configuration dictionary if view exists, None otherwise
            
        Example:
            >>> config_mgr = ConfigManager(athena, "account_map")
            >>> config = config_mgr.load_from_view("my_database")
            >>> if config:
            ...     print(config['metadata']['source_table'])
        """
        if not self.config_view_exists(database):
            logger.info("Config view %s.%s does not exist", database, self.config_view_name)
            return None
        
        try:
            logger.info("Loading configuration from %s.%s", database, self.config_view_name)
            
            # Query the config view
            query = f'SELECT * FROM "{database}"."{self.config_view_name}"'
            results = self.athena.query(
                sql=query,
                database=database,
                include_header=True
            )
            
            if not results or len(results) < 2:
                logger.warning("Config view is empty")
                return None
            
            # Convert results to list of dicts
            header = results[0]
            rows = []
            for row in results[1:]:
                row_dict = dict(zip(header, row))
                rows.append(row_dict)
            
            # Parse rows into config structure
            config = self.parse_config_rows(rows)
            logger.info("Successfully loaded configuration with %d taxonomy dimensions", 
                       len(config.get('taxonomy_dimensions', [])))
            
            return config
            
        except BaseException as e:
            logger.error("Failed to load configuration from view: %s", str(e), exc_info=True)
            return None
    
    def save_to_view(self, config: dict, database: str) -> bool:
        """
        Save configuration to Athena config view.
        
        Args:
            config: Configuration dictionary to save
            database: Target database for the config view
            
        Returns:
            True if save was successful, False otherwise
            
        Example:
            >>> config = {
            ...     'metadata': {
            ...         'source_table': 'organization_data',
            ...         'source_database': 'cur_database'
            ...     },
            ...     'taxonomy_dimensions': [
            ...         {'name': 'business_unit', 'source_type': 'tag', 'source_value': 'BusinessUnit'}
            ...     ]
            ... }
            >>> config_mgr.save_to_view(config, "my_database")
            True
        """
        try:
            logger.info("Saving configuration to %s.%s", database, self.config_view_name)
            
            # Generate config rows from config dict
            rows = self.generate_config_rows(config)
            
            # Generate SQL for config view
            sql = self._generate_config_view_sql(rows, database)
            sql_size = len(sql.encode('utf-8'))
            
            logger.info("Generated config view SQL (%d bytes)", sql_size)
            
            # Check if we need to split the view
            if sql_size > 262144:  # Athena's max SQL size
                logger.info("Config view SQL exceeds Athena size limit, creating separate views due to size limits")
                return self._create_split_config_view(rows, database)
            else:
                # Create single config view
                self.athena.query(sql=sql, database=database)
                logger.info("Successfully created config view")
                return True
                
        except Exception as e:
            logger.error("Failed to save configuration to view: %s", str(e), exc_info=True)
            return False
    
    def config_view_exists(self, database: str) -> bool:
        """
        Check if configuration view exists in the database.
        
        Args:
            database: Database to check
            
        Returns:
            True if config view exists, False otherwise
        """
        try:
            # Try to query the view directly - most reliable method
            check_sql = f'SELECT * FROM "{database}"."{self.config_view_name}" LIMIT 1'
            results = self.athena.query(
                sql=check_sql,
                database=database,
                include_header=False
            )
            
            # If query succeeds, view exists (even if empty)
            logger.info("Config view %s.%s exists", database, self.config_view_name)
            return True
            
        except BaseException as e:
            # CidCritical extends BaseException, so we must catch BaseException
            error_msg = str(e).lower()
            if 'does not exist' in error_msg or 'not found' in error_msg or 'table_not_found' in error_msg:
                logger.info("Config view %s.%s does not exist", database, self.config_view_name)
                return False
            else:
                logger.warning("Could not check if config view exists: %s", str(e))
                return False
    
    def parse_config_rows(self, rows: List[dict]) -> dict:
        """
        Parse config rows from view into structured configuration dictionary.
        
        The config view uses a two-row-type structure:
        - Metadata rows: config_type='metadata', stores file_source_view, source_table, source_database
        - Dimension rows: config_type='dimension', stores key_name, source_type, source_value
        
        For name_split dimensions, source_value is JSON-encoded dict with separator and index.
        
        Args:
            rows: List of row dictionaries from config view
            
        Returns:
            Structured configuration dictionary
            
        Example:
            >>> rows = [
            ...     {'config_type': 'metadata', 'key_name': 'source_table', 
            ...      'source_type': 'source', 'source_value': 'organization_data'},
            ...     {'config_type': 'dimension', 'key_name': 'business_unit',
            ...      'source_type': 'tag', 'source_value': 'BusinessUnit'}
            ... ]
            >>> config = config_mgr.parse_config_rows(rows)
            >>> config['metadata']['source_table']
            'organization_data'
        """
        import json
        
        config = {
            'metadata': {},
            'taxonomy_dimensions': []
        }
        
        for row in rows:
            config_type = row.get('config_type', '')
            key_name = row.get('key_name', '')
            source_type = row.get('source_type', '')
            source_value = row.get('source_value', '')
            
            if config_type == 'metadata':
                # Store metadata fields
                config['metadata'][key_name] = source_value
            elif config_type == 'dimension':
                # Parse source_value for name_split type
                parsed_value = source_value
                if source_type == 'name_split':
                    try:
                        parsed_value = json.loads(source_value)
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse name_split source_value for {key_name}: {source_value}")
                        parsed_value = source_value
                
                # Store taxonomy dimension
                dimension = {
                    'name': key_name,
                    'source_type': source_type,
                    'source_value': parsed_value
                }
                config['taxonomy_dimensions'].append(dimension)
            elif config_type == 'payer_name':
                # Reconstruct payer_names mapping
                if 'payer_names' not in config:
                    config['payer_names'] = {}
                config['payer_names'][key_name] = source_value
        
        # Reconstruct file_source from metadata if file_source_view exists
        if config['metadata'].get('file_source_view'):
            has_file_dimensions = any(
                d['source_type'] == 'file' for d in config['taxonomy_dimensions']
            )
            if has_file_dimensions:
                config['file_source'] = {
                    'use_existing_view': True
                }
            else:
                # file_source_view in metadata but no file dimensions — still reconstruct
                # This can happen if dimensions were removed but metadata wasn't cleaned up
                logger.info("file_source_view found in metadata but no file dimensions")
                config['file_source'] = {
                    'use_existing_view': True
                }
        elif any(d['source_type'] == 'file' for d in config['taxonomy_dimensions']):
            # No file_source_view in metadata but file dimensions exist
            # Reconstruct with default view name
            logger.info("File dimensions found but no file_source_view in metadata, reconstructing")
            config['file_source'] = {
                'use_existing_view': True
            }
        
        return config
    
    def generate_config_rows(self, config: dict) -> List[dict]:
        """
        Generate config rows from structured configuration dictionary.
        
        For name_split dimensions, source_value dict is JSON-encoded.
        
        Args:
            config: Configuration dictionary with metadata and taxonomy_dimensions
            
        Returns:
            List of row dictionaries for config view
            
        Example:
            >>> config = {
            ...     'metadata': {'source_table': 'organization_data'},
            ...     'taxonomy_dimensions': [
            ...         {'name': 'business_unit', 'source_type': 'tag', 'source_value': 'BusinessUnit'}
            ...     ]
            ... }
            >>> rows = config_mgr.generate_config_rows(config)
            >>> rows[0]['config_type']
            'metadata'
        """
        import json
        
        rows = []
        
        # Generate metadata rows
        metadata = config.get('metadata', {})
        for key_name, source_value in metadata.items():
            rows.append({
                'config_type': 'metadata',
                'key_name': key_name,
                'source_type': 'source',
                'source_value': str(source_value)
            })
        
        # Generate dimension rows
        taxonomy_dimensions = config.get('taxonomy_dimensions', [])
        for dimension in taxonomy_dimensions:
            source_value = dimension.get('source_value', '')
            source_type = dimension.get('source_type', '')
            
            # JSON-encode dict values (for name_split)
            if isinstance(source_value, dict):
                source_value = json.dumps(source_value)
            else:
                source_value = str(source_value)
            
            rows.append({
                'config_type': 'dimension',
                'key_name': dimension.get('name', ''),
                'source_type': source_type,
                'source_value': source_value
            })
        
        # Generate payer_name rows
        payer_names = config.get('payer_names', {})
        for payer_id, payer_name in payer_names.items():
            rows.append({
                'config_type': 'payer_name',
                'key_name': str(payer_id),
                'source_type': 'name',
                'source_value': str(payer_name)
            })
        
        return rows
    
    def _generate_config_view_sql(self, rows: List[dict], database: str) -> str:
        """
        Generate CREATE OR REPLACE VIEW SQL for config view using VALUES clause.
        
        Args:
            rows: List of row dictionaries
            database: Target database
            
        Returns:
            Complete SQL statement
        """
        # Generate VALUES rows
        values_rows = []
        for row in rows:
            config_type = row['config_type'].replace("'", "''")
            key_name = row['key_name'].replace("'", "''")
            source_type = row['source_type'].replace("'", "''")
            source_value = row['source_value'].replace("'", "''")
            
            values_rows.append(
                f"('{config_type}', '{key_name}', '{source_type}', '{source_value}')"
            )
        
        values_clause = ',\n  '.join(values_rows)
        
        # Build complete SQL
        sql = f"""CREATE OR REPLACE VIEW {database}.{self.config_view_name} AS
SELECT * FROM (
  VALUES
  {values_clause}
) AS t (config_type, key_name, source_type, source_value)"""
        
        return sql
    
    def _create_split_config_view(self, rows: List[dict], database: str) -> bool:
        """
        Create split config views when SQL exceeds size limit.
        
        Args:
            rows: List of row dictionaries
            database: Target database
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Calculate how many rows per part
            chunk_size = len(rows) // 2
            part_num = 1
            part_views = []
            
            while chunk_size > 0:
                # Try splitting with current chunk size
                chunks = [rows[i:i + chunk_size] for i in range(0, len(rows), chunk_size)]
                
                # Check if all chunks fit
                all_fit = True
                for chunk in chunks:
                    part_view_name = f"{self.config_view_name}_part{part_num}"
                    sql = self._generate_config_view_sql(chunk, database)
                    sql_size = len(sql.encode('utf-8'))
                    
                    if sql_size > 262144:
                        all_fit = False
                        chunk_size = chunk_size // 2
                        break
                
                if all_fit:
                    # Create all part views
                    part_num = 1
                    for chunk in chunks:
                        part_view_name = f"{self.config_view_name}_part{part_num}"
                        sql = self._generate_config_view_sql(chunk, database)
                        self.athena.query(sql=sql, database=database)
                        part_views.append(part_view_name)
                        logger.info("Created config view part %d", part_num)
                        part_num += 1
                    
                    # Create UNION view
                    union_parts = [f'SELECT * FROM {database}."{vn}"' for vn in part_views]
                    union_query = '\nUNION ALL\n'.join(union_parts)
                    union_sql = f"CREATE OR REPLACE VIEW {database}.{self.config_view_name} AS\n{union_query}"
                    
                    self.athena.query(sql=union_sql, database=database)
                    logger.info("Created UNION config view from %d parts", len(part_views))
                    
                    return True
            
            logger.error("Could not split config view into small enough parts")
            return False
            
        except Exception as e:
            logger.error("Failed to create split config view: %s", str(e), exc_info=True)
            return False
    
    def validate_config(self, config: dict) -> Tuple[bool, List[str]]:
        """
        Validate configuration completeness and correctness.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
            
        Example:
            >>> config = {'metadata': {}, 'taxonomy_dimensions': []}
            >>> is_valid, errors = config_mgr.validate_config(config)
            >>> if not is_valid:
            ...     print("Errors:", errors)
        """
        errors = []
        
        # Check required metadata fields
        metadata = config.get('metadata', {})
        required_metadata = ['source_table', 'source_database']
        
        for field in required_metadata:
            if field not in metadata or not metadata[field]:
                errors.append(f"Missing required metadata field: {field}")
        
        # Check taxonomy dimensions
        taxonomy_dimensions = config.get('taxonomy_dimensions', [])
        
        if not taxonomy_dimensions:
            errors.append("No taxonomy dimensions configured")
        
        # Validate dimension names are valid SQL identifiers
        dimension_names = set()
        for dimension in taxonomy_dimensions:
            name = dimension.get('name', '')
            
            if not name:
                errors.append("Taxonomy dimension missing name")
                continue
            
            # Check for valid SQL identifier
            if not self._is_valid_sql_identifier(name):
                errors.append(f"Invalid SQL identifier for dimension name: {name}")
            
            # Check for duplicates
            if name in dimension_names:
                errors.append(f"Duplicate taxonomy dimension name: {name}")
            dimension_names.add(name)
            
            # Check required fields
            if not dimension.get('source_type'):
                errors.append(f"Dimension {name} missing source_type")
            if not dimension.get('source_value'):
                errors.append(f"Dimension {name} missing source_value")
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def _is_valid_sql_identifier(self, name: str) -> bool:
        """
        Check if a name is a valid SQL identifier.
        
        Args:
            name: Name to validate
            
        Returns:
            True if valid, False otherwise
        """
        import re
        
        # SQL identifier rules:
        # - Must start with letter or underscore
        # - Can contain letters, digits, underscores
        # - Cannot be a SQL reserved word
        
        if not name:
            return False
        
        # Check pattern
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            return False
        
        # Check against common SQL reserved words
        reserved_words = {
            'select', 'from', 'where', 'insert', 'update', 'delete', 'create',
            'drop', 'alter', 'table', 'view', 'index', 'database', 'schema',
            'grant', 'revoke', 'union', 'join', 'inner', 'outer', 'left', 'right',
            'on', 'as', 'and', 'or', 'not', 'null', 'true', 'false', 'order',
            'group', 'by', 'having', 'limit', 'offset', 'distinct', 'all', 'any',
            'exists', 'in', 'between', 'like', 'is', 'case', 'when', 'then', 'else',
            'end', 'cast', 'convert'
        }
        
        if name.lower() in reserved_words:
            return False
        
        return True
    
    def _validate_dimension_name(self, name: str) -> bool:
        """
        Validation function for dimension names in inquirer prompts.
        
        Args:
            name: Name to validate
            
        Returns:
            True if valid, or error message string if invalid
        """
        if not name or len(name.strip()) == 0:
            return "Dimension name cannot be empty"
        
        name = name.strip()
        
        if not self._is_valid_sql_identifier(name):
            return "Invalid SQL identifier. Must start with letter/underscore, contain only letters/digits/underscores, no spaces or special characters"
        
        return True
    
    def _sanitize_dimension_name(self, name: str) -> str:
        """
        Sanitize a dimension name by replacing spaces with underscores.
        
        Args:
            name: Name to sanitize
            
        Returns:
            Sanitized name with spaces replaced by underscores
        """
        if not name:
            return name
        
        # Replace spaces with underscores
        sanitized = name.strip().replace(' ', '_')
        
        return sanitized


class AutoDiscovery:
    """
    Handles automatic discovery of databases, tables, tag keys, and file columns.
    
    The AutoDiscovery class provides intelligent auto-selection logic that automatically
    chooses resources when only one option exists, and presents interactive prompts
    when multiple options are available.
    
    Attributes:
        athena (Athena): CID Athena helper instance for querying metadata
    """
    
    def __init__(self, athena: Athena):
        """
        Initialize AutoDiscovery with Athena helper.
        
        Args:
            athena: CID Athena helper instance
        """
        self.athena = athena
    
    def discover_source(self) -> Tuple[str, str]:
        """
        Scan all Athena databases for an 'organization_data' table.

        If exactly one database contains the table, auto-selects it.
        If multiple databases contain it, prompts the user to choose.
        If none found, falls back to manual database + table selection.

        Returns:
            Tuple of (database, table)
        """
        from InquirerPy import inquirer as inq

        logger.info("Scanning all databases for 'organization_data' table...")
        databases = self.athena.list_databases()

        if not databases:
            raise CidCritical("No databases found in Athena")

        # Scan each database for organization_data
        matches = []
        for db in databases:
            try:
                metadata = self.athena.get_table_metadata('organization_data', db)
                if metadata:
                    matches.append(db)
            except Exception:
                continue

        if len(matches) == 1:
            logger.info(f"Found 'organization_data' in database '{matches[0]}' — auto-selected")
            return matches[0], 'organization_data'
        elif len(matches) > 1:
            logger.info(f"Found 'organization_data' in {len(matches)} databases: {matches}")
            db = inq.select(
                message="Multiple databases contain 'organization_data'. Select source database:",
                choices=matches
            ).execute()
            return db, 'organization_data'
        else:
            # Not found anywhere — fall back to manual selection
            logger.warning("'organization_data' table not found in any database")
            db = self.discover_databases()
            table = self.discover_tables(db)
            return db, table

    def discover_target_database(self, databases: Optional[List[str]] = None,
                                  source_database: Optional[str] = None) -> str:
        """
        Discover and confirm the target database for storing views.

        Suggests a database with 'cur' in the name if one exists.
        Otherwise suggests the source database. Lets the user confirm or pick another.

        Args:
            databases: Optional pre-fetched list of databases
            source_database: The source database (used as fallback suggestion)

        Returns:
            Selected target database name
        """
        from InquirerPy import inquirer as inq

        if databases is None:
            databases = self.athena.list_databases()

        if not databases:
            raise CidCritical("No databases found in Athena")

        # Find a database with 'cur' in the name
        cur_dbs = [db for db in databases if 'cur' in db.lower()]

        # Priority 1: find a database that already has an account_map view
        account_map_db = None
        for db in databases:
            try:
                metadata = self.athena.get_table_metadata('account_map', db)
                if metadata:
                    account_map_db = db
                    break
            except Exception:
                continue

        if account_map_db:
            suggested = account_map_db
        elif cur_dbs:
            suggested = cur_dbs[0]
        elif source_database and source_database in databases:
            suggested = source_database
        else:
            suggested = databases[0]

        print(f"\n📦 Choose target database for account_map, suggesting {suggested}")
        confirm = inq.confirm(
            message=f"Use the suggested database '{suggested}' to write account_map to?",
            default=True
        ).execute()

        if confirm:
            return suggested

        return inq.select(
            message="Select target database for account_map deployment:",
            choices=databases,
            default=suggested
        ).execute()

    def discover_databases(self, preferred: Optional[str] = None) -> str:
        """
        Discover and select database with auto-selection logic.
        
        If a preferred database is provided and exists, it will be used.
        If only one database exists, it will be auto-selected.
        If multiple databases exist, an interactive prompt will be shown.
        
        Args:
            preferred: Optional preferred database name to validate and use
            
        Returns:
            Selected database name
            
        Raises:
            Exception: If no databases are found
        """
        # Validate preferred database if provided
        if preferred:
            if self.athena.get_database(preferred):
                logger.info(f"Using preferred database: {preferred}")
                return preferred
            else:
                logger.warning(f"Preferred database '{preferred}' not found, discovering alternatives...")
        
        # Get list of available databases
        databases = self.athena.list_databases()
        
        if len(databases) == 0:
            raise Exception("No databases found in Athena")
        elif len(databases) == 1:
            logger.info(f"Auto-selected database: {databases[0]}")
            return databases[0]
        else:
            # Multiple databases - prompt user to select
            # Set default to "optimization_data" if it exists, otherwise first database
            default_db = "optimization_data" if "optimization_data" in databases else databases[0]
            return inquirer.select(
                message="Select source database:",
                choices=databases,
                default=default_db
            ).execute()
    
    def discover_tables(self, database: str, preferred: Optional[str] = None) -> str:
        """
        Discover and select table with auto-selection logic.
        
        If a preferred table is provided and exists, it will be used.
        If only one table exists in the database, it will be auto-selected.
        If multiple tables exist, an interactive prompt will be shown.
        
        Args:
            database: Database name to search for tables
            preferred: Optional preferred table name to validate and use
            
        Returns:
            Selected table name
            
        Raises:
            Exception: If no tables are found in the database
        """
        # Validate preferred table if provided
        if preferred:
            metadata = self.athena.get_table_metadata(preferred, database)
            if metadata:
                logger.info(f"Using preferred table: {preferred}")
                return preferred
            else:
                logger.warning(f"Preferred table '{preferred}' not found in database '{database}', discovering alternatives...")
        
        # Get list of available tables
        table_metadata = self.athena.list_table_metadata(database)
        
        if not table_metadata or len(table_metadata) == 0:
            raise Exception(f"No tables found in database '{database}'")
        
        # Extract table names from metadata
        tables = [table['Name'] for table in table_metadata]
        
        if len(tables) == 1:
            logger.info(f"Auto-selected table: {tables[0]}")
            return tables[0]
        else:
            # Multiple tables - prompt user to select
            # Set default to "organization_data" if it exists, otherwise first table
            default_table = "organization_data" if "organization_data" in tables else tables[0]
            return inquirer.select(
                message=f"Select source table from database '{database}':",
                choices=tables,
                default=default_table
            ).execute()
    
    def discover_tag_keys(self, database: str, table: str, tag_column: str = 'hierarchytags') -> List[str]:
        """
        Discover available tag keys from the source table.
        
        Queries sample rows from the table and parses the tag column to extract
        all unique tag keys.
        
        Args:
            database: Database name containing the table
            table: Table name to query
            tag_column: Name of the column containing tags (default: 'hierarchytags')
            
        Returns:
            Sorted list of unique tag keys
            
        Raises:
            Exception: If unable to query the table or parse tags
        """
        logger.info(f"Discovering tag keys from {database}.{table}.{tag_column}")
        
        try:
            # Query sample rows to extract tag keys
            query = f"""
                SELECT {tag_column}
                FROM "{database}"."{table}"
                WHERE {tag_column} IS NOT NULL
                LIMIT 100
            """
            
            results = self.athena.query(query)
            
            if not results:
                logger.warning(f"No data found in {tag_column} column")
                return []
            
            # Extract unique tag keys
            tag_keys = set()
            
            for row in results:
                tags_value = row[0] if row else None
                
                if not tags_value:
                    continue
                
                # Parse tags - handle both string and list formats
                if isinstance(tags_value, str):
                    parsed_tags = parse_athena_tags(tags_value)
                    for tag_item in parsed_tags:
                        if 'key' in tag_item:
                            tag_keys.add(tag_item['key'])
                elif isinstance(tags_value, list):
                    for tag_item in tags_value:
                        if isinstance(tag_item, dict) and 'key' in tag_item:
                            tag_keys.add(tag_item['key'])
            
            tag_keys_list = sorted(list(tag_keys))
            logger.info(f"Found {len(tag_keys_list)} unique tag keys")
            
            return tag_keys_list
            
        except Exception as e:
            logger.error(f"Failed to discover tag keys: {str(e)}")
            raise
    
    def discover_account_id_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Auto-detect account ID column in dataframe using pattern matching.
        
        Checks for common account ID column name patterns (case-insensitive):
        - account_id
        - accountid
        - account
        - id
        - aws_account_id
        
        Args:
            df: DataFrame to search for account ID column
            
        Returns:
            Name of the detected account ID column, or None if not found
        """
        # Common account ID column patterns (case-insensitive)
        patterns = [
            'account_id',
            'accountid', 
            'account',
            'id',
            'aws_account_id'
        ]
        
        # Get lowercase column names for comparison
        columns_lower = {col.lower(): col for col in df.columns}
        
        # Check each pattern in order
        for pattern in patterns:
            if pattern in columns_lower:
                detected_column = columns_lower[pattern]
                logger.info(f"Auto-detected account ID column: {detected_column}")
                return detected_column
        
        logger.warning("No account ID column auto-detected")
        return None
    
    def prompt_file_selection(self, extensions: Optional[List[str]] = None) -> str:
        """
        Present file selection using glob search and selection list.
        
        Searches for files with specified extensions and presents them as a list.
        Defaults to common data file formats: .json, .csv, .xlsx, .xls
        
        Args:
            extensions: List of file extensions to filter (e.g., ['.json', '.csv'])
                       If None, defaults to ['.json', '.csv', '.xlsx', '.xls']
            
        Returns:
            Selected file path
        """
        from InquirerPy import inquirer
        import glob
        
        if extensions is None:
            extensions = ['.json', '.csv', '.xlsx', '.xls']
        
        # Ensure extensions start with dot
        extensions = [ext if ext.startswith('.') else f'.{ext}' for ext in extensions]
        
        logger.info(f"Searching for files with extensions: {extensions}")
        
        # Search for files with specified extensions
        matching_files = []
        for ext in extensions:
            # Search in current directory and subdirectories
            pattern = f"**/*{ext}"
            files = glob.glob(pattern, recursive=True)
            matching_files.extend(files)
        
        # Remove duplicates and sort
        matching_files = sorted(set(matching_files))
        
        if not matching_files:
            logger.warning(f"No files found with extensions: {', '.join(extensions)}")
            # Fall back to manual entry
            selected_file = inquirer.text(
                message=f"No files found. Enter file path manually:",
                validate=lambda path: Path(path).is_file() or "File does not exist"
            ).execute()
        else:
            # Add option to enter path manually
            choices = matching_files + ["[Enter path manually]"]
            
            selected = inquirer.select(
                message=f"Select file ({', '.join(extensions)}):",
                choices=choices,
                default=choices[0] if matching_files else None
            ).execute()
            
            if selected == "[Enter path manually]":
                selected_file = inquirer.text(
                    message="Enter file path:",
                    validate=lambda path: Path(path).is_file() or "File does not exist"
                ).execute()
            else:
                selected_file = selected
        
        logger.info(f"Selected file: {selected_file}")
        return str(selected_file)


class UnifiedWorkflow:
    """
    Orchestrates the complete account mapping workflow from discovery through execution.

    This class integrates AutoDiscovery, ConfigManager, DataLoader, TransformEngine,
    and AthenaWriter to provide a streamlined workflow for creating and updating
    account mapping views.
    """

    def __init__(self, athena: Athena, view_name: str = "account_map"):
        """
        Initialize the unified workflow controller.

        Args:
            athena: Athena helper instance for database operations
            view_name: Base name for the account map view (default: "account_map")
        """
        self.athena = athena
        self.view_name = view_name
        self.discovery = AutoDiscovery(athena)
        self.config_mgr = ConfigManager(athena, view_name)
        self.data_loader = None  # Initialized after config is determined
        self.writer = None  # Initialized after config is determined

    @staticmethod
    def _checkbox_with_retry(message: str, choices: list, **kwargs) -> list:
        """
        Wrapper around inquirer.checkbox that confirms empty selection.

        InquirerPy checkboxes require Space to toggle items and Enter to confirm.
        Users often press Enter without selecting anything. This helper detects
        that and asks if they want to try again.

        Args:
            message: Prompt message
            choices: List of choices
            **kwargs: Additional kwargs passed to inquirer.checkbox

        Returns:
            list: Selected items (may be empty if user confirms)
        """
        from InquirerPy import inquirer as inq

        while True:
            result = inq.checkbox(
                message=message,
                choices=choices,
                **kwargs
            ).execute()

            if result:
                return result

            # Empty selection — confirm intent
            print("\n💡 Tip: Use Space to select items, then Enter to confirm.")
            retry = inq.confirm(
                message="Nothing was selected. Try again?",
                default=True
            ).execute()

            if not retry:
                return result

    def _resolve_dimension_name(self, name: str, config: dict) -> str:
        """
        Check if a dimension name already exists and prompt for a new name if so.

        Args:
            name: Proposed dimension name
            config: Current configuration dict with taxonomy_dimensions

        Returns:
            Unique dimension name (original or user-provided replacement)
        """
        from InquirerPy import inquirer as inq

        existing_names = {d['name'] for d in config.get('taxonomy_dimensions', [])}

        while name in existing_names:
            print(f"\n⚠️  Dimension name '{name}' already exists.")
            name = inq.text(
                message=f"Enter a different name for this dimension:",
                default=f"{name}_2",
                validate=lambda x: (len(x.strip()) > 0) or "Name cannot be empty"
            ).execute()
            name = self.config_mgr._sanitize_dimension_name(name)

        return name

    def execute(self, source_file: str = None) -> dict:
        """
        Execute the complete workflow with auto-discovery.

        This method orchestrates the entire account mapping process:
        1. Discover source (organization_data table) and target database
        2. Check for existing configuration
        3. Prompt for configuration reuse or create new configuration
        4. Load data from Athena and optionally from file
        5. Transform data according to configuration
        6. Preview and confirm SQL
        7. Write views to Athena

        Args:
            source_file: Optional path to file for file-based taxonomy dimensions

        Returns:
            dict: Results containing created view names and status
        """
        # Phase 1: Discovery — find source (organization_data) and target database
        source_database, table = self.discovery.discover_source()
        target_database = self.discovery.discover_target_database(
            source_database=source_database
        )

        logger.info(f"Source: {source_database}.{table}")
        logger.info(f"Target database for views: {target_database}")

        # Pre-set Athena parameters BEFORE any queries to avoid prompts
        from cid.utils import set_parameters, get_parameters
        params = get_parameters()
        params['athena-database'] = target_database
        
        # Auto-select workgroup if only one real workgroup exists (excluding "create new" options)
        workgroups = [wg['Name'] for wg in self.athena.list_work_groups()]
        # Filter out any that might be added by the UI as "create new" options
        real_workgroups = [wg for wg in workgroups if not wg.endswith('(create new)')]
        
        if len(real_workgroups) == 1:
            logger.info(f"Auto-selected workgroup: {real_workgroups[0]}")
            params['athena-workgroup'] = real_workgroups[0]
        elif len(real_workgroups) == 0:
            # No workgroups exist, use default 'primary'
            logger.info("No workgroups found, using default 'primary'")
            params['athena-workgroup'] = 'primary'
        
        set_parameters(params)

        # Phase 2: Configuration — load/save config from target database
        existing_config = self._check_existing_config(target_database)

        if existing_config:
            if self._prompt_config_reuse(existing_config):
                config = existing_config
                # If config has file source, ask if user wants to update it
                if config.get('file_source'):
                    config = self._handle_existing_file_source(config, target_database, table, source_file=source_file)
            else:
                config = self._interactive_configuration(source_database, table, source_file=source_file)
        else:
            config = self._interactive_configuration(source_database, table, source_file=source_file)

        # Store source and target in config for later use
        config['metadata']['source_database'] = source_database
        config['metadata']['source_table'] = table
        config['metadata']['target_database'] = target_database
        
        # Also store in athena section for DataLoader compatibility
        if 'athena' not in config:
            config['athena'] = {}
        config['athena']['database'] = source_database
        config['athena']['table'] = table

        # Initialize data loader and writer with config
        self.data_loader = DataLoader(self.athena, config)
        self.writer = AthenaWriter(config, self.athena)

        # Phase 3: Data Loading
        logger.info("Loading organization data from Athena...")
        org_data = self.data_loader.load_from_athena()

        # Payer naming: prompt user to name management account IDs
        if not config.get('payer_names'):
            config = self._prompt_payer_names(config, org_data)

        file_data = None
        if config.get('file_source'):
            # Check if file data is already loaded (from interactive config)
            if 'data' in config['file_source']:
                logger.info("Using file data from configuration")
                file_data = config['file_source']['data']
            elif 'path' in config['file_source']:
                logger.info(f"Loading file data from {config['file_source']['path']}...")
                file_data = self.data_loader.load_from_file(config['file_source']['path'])
            else:
                # File source exists but no data - using existing view
                logger.info("File source configured to use existing Athena view")
                file_data = None

        # Phase 4: Transformation
        logger.info("Transforming data according to configuration...")
        transform_engine = TransformEngine(config, org_data, file_data)
        transformed_data = transform_engine.transform()

        # Add payer_name column to preview if payer names are configured
        payer_names = config.get('payer_names', {})
        if payer_names and 'payer_id' in transformed_data.columns:
            transformed_data['payer_name'] = transformed_data['payer_id'].map(
                lambda pid: payer_names.get(str(pid).zfill(12), str(pid)) if pd.notna(pid) else pid
            )

        # Reorder columns: fixed prefix, then remaining sorted alphabetically
        fixed_cols = ['account_id', 'account_name', 'payer_id', 'payer_name']
        prefix = [c for c in fixed_cols if c in transformed_data.columns]
        rest = sorted(
            [c for c in transformed_data.columns if c not in fixed_cols],
            key=str.lower
        )
        transformed_data = transformed_data[prefix + rest]

        # Phase 5: Preview and Confirmation
        with spinner("Generating preview"):
            sql = self.writer._generate_account_map_transformation_sql(config, self.view_name, target_database)
            sample = transformed_data.head(10)

        if not self._preview_and_confirm(sql, sample):
            return {"status": "cancelled", "message": "User cancelled operation"}

        # Phase 6: Write Views
        logger.info("Writing views to Athena...")
        results = self.writer.write_complete_mapping(config, transformed_data, target_database)
        results['status'] = 'success'

        return results

    def _discover_database(self, database: Optional[str]) -> str:
        """
        Auto-discover or prompt for database selection.

        Args:
            database: Optional preferred database name

        Returns:
            str: Selected database name
        """
        return self.discovery.discover_databases(database)

    def _discover_table(self, database: str, table: Optional[str]) -> str:
        """
        Auto-discover or prompt for table selection.

        Args:
            database: Database name to search in
            table: Optional preferred table name

        Returns:
            str: Selected table name
        """
        return self.discovery.discover_tables(database, table)

    def _check_existing_config(self, database: str) -> Optional[dict]:
        """
        Check for existing configuration view.

        Args:
            database: Database name to check in

        Returns:
            Optional[dict]: Existing configuration or None if not found
        """
        return self.config_mgr.load_from_view(database)

    def _prompt_config_reuse(self, config: dict) -> bool:
        """
        Display config summary and prompt for reuse.

        Args:
            config: Existing configuration to display

        Returns:
            bool: True if user wants to reuse config, False otherwise
        """
        from InquirerPy import inquirer

        print("\n" + "="*60)
        print("📋 Existing Configuration Found")
        print("="*60)

        source_db = config['metadata'].get('source_database')
        source_tbl = config['metadata'].get('source_table')
        target_db = config['metadata'].get('target_database')

        print(f"\n  Source table : {source_db}.{source_tbl}")
        if target_db:
            print(f"  Target database : {target_db}")

        if config.get('file_source'):
            print(f"  File source view: {config['metadata'].get('file_source_view')}")

        dims = config.get('taxonomy_dimensions', [])
        if dims:
            print(f"\n  Taxonomy Dimensions ({len(dims)}):")
            print("  " + "-"*56)
            for i, dim in enumerate(dims, 1):
                source_type = dim['source_type']
                source_value = dim['source_value']

                if source_type == 'name_split' and isinstance(source_value, dict):
                    created_from = f"Account name split by \"{source_value.get('separator')}\" at index {source_value.get('index')}"
                elif source_type == 'file':
                    created_from = f"File column \"{source_value}\""
                elif source_type == 'tag':
                    created_from = f"Tag with key \"{source_value}\""
                else:
                    created_from = f"{source_type}: {source_value}"

                print(f"    Dimension {i}:")
                print(f"      Column name  : {dim['name']}")
                print(f"      Created from : {created_from}")

        payer_names = config.get('payer_names', {})
        if payer_names:
            print(f"\n  Payer Account Names ({len(payer_names)}):")
            print("  " + "-"*56)
            for pid, pname in payer_names.items():
                print(f"    {pid} → {pname}")

        print("\n" + "="*60 + "\n")

        return inquirer.confirm(
            message="Use existing configuration?",
            default=True
        ).execute()

    def _handle_existing_file_source(self, config: dict, database: str, table: str,
                                     source_file: str = None) -> dict:
        """
        Handle existing file source configuration - ask if user wants to update it.

        Args:
            config: Existing configuration with file source
            database: Database name
            table: Table name
            source_file: Optional path to file provided via --file parameter

        Returns:
            dict: Updated configuration
        """
        from InquirerPy import inquirer

        file_source_view = config['metadata'].get('file_source_view', f"{self.view_name}_file_source")
        
        # Check if the file source view actually exists in Athena
        view_exists = False
        try:
            check_sql = f'SELECT * FROM "{database}"."{file_source_view}" LIMIT 1'
            self.athena.query(sql=check_sql, database=database, include_header=False, fail=False)
            view_exists = True
        except BaseException:
            view_exists = False
        
        if not view_exists:
            print(f"\n⚠️  File source view '{file_source_view}' no longer exists in Athena.")
            print("You need to provide the file again to recreate it.\n")
            update_file = True
        else:
            logger.info(f"\nThis configuration uses a file source view: {file_source_view}")
            
            update_file = inquirer.confirm(
                message="Do you want to update the file source with new data?",
                default=False
            ).execute()

        if update_file:
            # Run file selection workflow
            logger.info("Updating file source...")
            
            # Use --file parameter if provided, otherwise prompt
            if source_file:
                file_path = source_file
                print(f"Using file: {source_file}")
            else:
                file_path = self.discovery.prompt_file_selection()

            # Load file to get columns
            temp_loader = DataLoader(self.athena, config)
            file_df = temp_loader.load_from_file(file_path)

            # Auto-detect or prompt for account ID column
            account_col = self.discovery.discover_account_id_column(file_df)
            if not account_col:
                account_col = inquirer.select(
                    message="Select the account ID column:",
                    choices=list(file_df.columns)
                ).execute()
            else:
                logger.info(f"Auto-detected account ID column: {account_col}")

            # Get file columns for dimensions (excluding account ID column)
            file_columns = [col for col in file_df.columns if col != account_col]

            # Ask which columns to use as taxonomy dimensions
            if file_columns:
                selected_columns = self._checkbox_with_retry(
                    message="Select which columns to use as taxonomy dimensions (space to select, Enter to confirm):",
                    choices=file_columns
                )

                if selected_columns:
                    # Prompt for dimension name customization
                    customize = inquirer.confirm(
                        message="Do you want to rename any taxonomy dimensions from the file?",
                        default=False
                    ).execute()

                    dimension_names = {}
                    if customize:
                        for col in selected_columns:
                            new_name = inquirer.text(
                                message=f"Name for dimension '{col}' (press Enter to keep):",
                                default=col,
                                validate=lambda x: self.config_mgr._validate_dimension_name(x) if x and x.strip() else True
                            ).execute()
                            # Sanitize the dimension name (replace spaces with underscores)
                            new_name = self.config_mgr._sanitize_dimension_name(new_name)
                            dimension_names[col] = new_name
                    else:
                        dimension_names = {col: col for col in selected_columns}

                    # Remove old file dimensions from config
                    config['taxonomy_dimensions'] = [
                        dim for dim in config['taxonomy_dimensions']
                        if dim['source_type'] != 'file'
                    ]

                    # Add new file dimensions to config
                    for col, name in dimension_names.items():
                        name = self._resolve_dimension_name(name, config)
                        config['taxonomy_dimensions'].append({
                            'name': name,
                            'source_type': 'file',
                            'source_value': col
                        })

            # Update file source info
            config['file_source'] = {
                'path': file_path,
                'account_column': account_col,
                'data': file_df
            }
            config['metadata']['file_source_view'] = file_source_view
        else:
            # Keep existing file source view - remove path/data so we don't try to reload
            logger.info(f"Keeping existing file source view: {file_source_view}")
            config['file_source'] = {
                'use_existing_view': True
            }
            config['metadata']['file_source_view'] = file_source_view

        return config

    def _interactive_configuration(self, database: str, table: str,
                                    source_file: str = None) -> dict:
        """
        Run interactive configuration workflow.

        Args:
            database: Database name
            table: Table name
            source_file: Optional path to file for file-based dimensions

        Returns:
            dict: New configuration
        """
        from InquirerPy import inquirer

        config = {
            'metadata': {
                'source_database': database,
                'source_table': table
            },
            'athena': {
                'database': database,
                'table': table
            },
            'taxonomy_dimensions': []
        }

        # Build data source choices — only include file option if --file was provided
        data_source_choices = ["Tags from source table"]
        if source_file:
            data_source_choices.append("Additional file")
        data_source_choices.append("Split account name column")

        # Single multi-select for all data source options
        data_sources = self._checkbox_with_retry(
            message="Select data sources for taxonomy dimensions (space to select, Enter to confirm):",
            choices=data_source_choices,
            default=["Tags from source table"]
        )

        use_tags = "Tags from source table" in data_sources
        use_file = "Additional file" in data_sources and source_file
        use_name_split = "Split account name column" in data_sources

        # Configure file source if selected
        if use_file:
            print("\n" + "="*70)
            print("📁 FILE SOURCE CONFIGURATION")
            print("="*70)
            print(f"Using file: {source_file}\n")
            
            # Use the provided file path directly
            file_path = source_file

            # Load file to get columns
            temp_loader = DataLoader(self.athena, config)
            file_df = temp_loader.load_from_file(file_path)

            # Auto-detect or prompt for account ID column
            account_col = self.discovery.discover_account_id_column(file_df)
            if not account_col:
                account_col = inquirer.select(
                    message="Select the account ID column:",
                    choices=list(file_df.columns)
                ).execute()
            else:
                logger.info(f"Auto-detected account ID column: {account_col}")

            # Store file source info
            config['file_source'] = {
                'path': file_path,
                'account_column': account_col,
                'data': file_df
            }
            config['metadata']['file_source_view'] = f"{self.view_name}_file_source"

            # Get file columns for dimensions (excluding account ID column)
            file_columns = [col for col in file_df.columns if col != account_col]

            # Ask which columns to use as taxonomy dimensions
            if file_columns:
                selected_columns = self._checkbox_with_retry(
                    message="Select which columns to use as taxonomy dimensions (space to select, Enter to confirm):",
                    choices=file_columns
                )

                if selected_columns:
                    # Prompt for dimension name customization
                    customize = inquirer.confirm(
                        message="Do you want to rename any taxonomy dimensions from the file?",
                        default=False
                    ).execute()

                    dimension_names = {}
                    if customize:
                        for col in selected_columns:
                            new_name = inquirer.text(
                                message=f"Name for dimension '{col}' (press Enter to keep):",
                                default=col
                            ).execute()
                            dimension_names[col] = new_name
                    else:
                        dimension_names = {col: col for col in selected_columns}

                    # Add file dimensions to config
                    for col, name in dimension_names.items():
                        name = self._resolve_dimension_name(name, config)
                        config['taxonomy_dimensions'].append({
                            'name': name,
                            'source_type': 'file',
                            'source_value': col
                        })

        # Configure name splitting if selected
        if use_name_split:
            print("\n" + "="*70)
            print("✂️  ACCOUNT NAME SPLIT CONFIGURATION")
            print("="*70)
            print("Extract taxonomy dimensions by splitting the account name.")
            print("Example: 'aws-account-awesomeproduct-prod'")
            print("  - Separator: '-'")
            print("  - Index 0: 'aws'")
            print("  - Index 1: 'account'")
            print("  - Index 2: 'awesomeproduct'")
            print("  - Index 3: 'prod'")
            print("\nEach dimension you create will become a column in the output.\n")
            
            # Keep adding split dimensions until user is done
            while True:
                separator = inquirer.text(
                    message="Enter separator character (e.g., '-', '_', '/') to split by:",
                    validate=lambda x: len(x) > 0 or "Separator cannot be empty"
                ).execute()

                index = inquirer.number(
                    message="Enter index to extract (0-based, e.g., 0 for first part, 1 for second):",
                    min_allowed=0
                ).execute()

                dim_name = inquirer.text(
                    message="Enter name for this taxonomy dimension:",
                    validate=lambda x: self.config_mgr._validate_dimension_name(x)
                ).execute()
                
                # Sanitize the dimension name (replace spaces with underscores)
                dim_name = self.config_mgr._sanitize_dimension_name(dim_name)
                dim_name = self._resolve_dimension_name(dim_name, config)

                config['taxonomy_dimensions'].append({
                    'name': dim_name,
                    'source_type': 'name_split',
                    'source_value': {
                        'separator': separator,
                        'index': int(index)
                    }
                })

                logger.info(f"Added dimension '{dim_name}' from name split by '{separator}' at index {index}")

                add_more = inquirer.confirm(
                    message="Add another name split dimension? (Each dimension = one output column)",
                    default=False
                ).execute()

                if not add_more:
                    break

        # Configure tags if selected
        if use_tags:
            print("\n" + "="*70)
            print("🏷️  TAG-BASED DIMENSIONS CONFIGURATION")
            print("="*70)
            print("Extract taxonomy dimensions from AWS resource tags in the source table.")
            print("Each selected tag will become a column in the output.\n")
            
            # Discover available tag keys
            logger.info("Discovering available tag keys...")
            tag_keys = self.discovery.discover_tag_keys(database, table)

            if tag_keys:
                logger.info(f"Found {len(tag_keys)} tag keys")

                # Prompt for tag-based dimensions
                selected_tags = self._checkbox_with_retry(
                    message="Select tag keys to use as taxonomy dimensions (space to select, Enter to confirm):",
                    choices=tag_keys
                )

                if selected_tags:
                    # Prompt for dimension name customization
                    customize = inquirer.confirm(
                        message="Do you want to customize taxonomy dimension names for tags?",
                        default=False
                    ).execute()

                    for tag in selected_tags:
                        if customize:
                            dim_name = inquirer.text(
                                message=f"Name for tag '{tag}' (press Enter to keep):",
                                default=tag,
                                validate=lambda x: self.config_mgr._validate_dimension_name(x) if x and x.strip() else True
                            ).execute()
                            # Sanitize the dimension name (replace spaces with underscores)
                            dim_name = self.config_mgr._sanitize_dimension_name(dim_name)
                        else:
                            dim_name = tag

                        dim_name = self._resolve_dimension_name(dim_name, config)
                        config['taxonomy_dimensions'].append({
                            'name': dim_name,
                            'source_type': 'tag',
                            'source_value': tag
                        })
            else:
                logger.warning("No tag keys found in the source table")

        # Validate configuration
        is_valid, errors = self.config_mgr.validate_config(config)
        if not is_valid:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            raise CidCritical("Invalid configuration")

        return config

    def _prompt_payer_names(self, config: dict, org_data: pd.DataFrame) -> dict:
        """
        Prompt user to assign friendly names to management account IDs.

        Discovers distinct payer IDs from org data and asks if the user wants
        to provide custom names. Stores the mapping in config['payer_names'].

        Args:
            config: Current configuration dictionary
            org_data: Organization DataFrame with payer_id column

        Returns:
            dict: Updated configuration with optional payer_names
        """
        from InquirerPy import inquirer

        if 'payer_id' not in org_data.columns:
            logger.debug("No payer_id column in org data, skipping payer naming")
            return config

        # Get distinct payer IDs
        unique_payers = org_data['payer_id'].dropna().unique()
        unique_payers = sorted([str(p).zfill(12) for p in unique_payers if p])

        if not unique_payers:
            return config

        print(f"\nFound {len(unique_payers)} management account(s): {', '.join(unique_payers)}")

        name_payers = inquirer.confirm(
            message="Do you want to assign friendly names to management accounts?",
            default=False
        ).execute()

        if name_payers:
            payer_names = {}
            for payer_id in unique_payers:
                name = inquirer.text(
                    message=f"Name for management account '{payer_id}' (Enter to skip):",
                    default=""
                ).execute()
                if name and name.strip():
                    payer_names[payer_id] = name.strip()

            if payer_names:
                config['payer_names'] = payer_names
                logger.info(f"Configured {len(payer_names)} payer name(s)")

        return config

    def _preview_and_confirm(self, sql: str, sample_data: pd.DataFrame) -> bool:
        """
        Display SQL and sample output for user confirmation.

        Args:
            sql: SQL query to display
            sample_data: Sample data to display

        Returns:
            bool: True if user confirms, False otherwise
        """
        from InquirerPy import inquirer

        print("\n" + "="*60)
        print("🔍 Preview: Account Map View SQL")
        print("="*60)
        print(sql)

        print("\n" + "="*60)
        print("📊 Preview: Sample Output (first 10 rows)")
        print("="*60)
        print(sample_data.to_string())
        print("="*60 + "\n")

        return inquirer.confirm(
            message="Create views with this configuration?",
            default=True
        ).execute()


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
            # Don't log SQL on error if it's large (file source views)
            log_sql = sql_size < 10000  # Only log if less than 10KB
            self._execute_view_creation(sql, database, log_sql_on_error=log_sql)
            return [view_name]
        else:
            # Need to split into multiple views
            logger.info("SQL size (%d bytes) exceeds limit (%d bytes), creating separate views due to size limits", sql_size, self.MAX_SQL_SIZE)
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
    
    def _drop_view(self, name: str, database: str) -> None:
        """
        Drop a view.
        
        Args:
            name: View name
            database: Database name
        """
        sql = f"DROP VIEW IF EXISTS {database}.{name}"
        self.athena_helper.query(sql=sql, database=database)
    
    def _drop_table(self, name: str, database: str) -> None:
        """
        Drop a table.
        
        Args:
            name: Table name
            database: Database name
        """
        sql = f"DROP TABLE IF EXISTS {database}.{name}"
        self.athena_helper.query(sql=sql, database=database)
    
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
        Split DataFrame and create multiple views with progress indicators.
        
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
                # All chunks fit, create the views with progress indicators
                total_parts = len(chunks)
                logger.info(f"Creating {total_parts} view parts...")
                
                for i, chunk in enumerate(chunks):
                    chunk_view_name = f"{view_name}_part{i + 1}"
                    # Drop existing view/table before creating
                    self._safe_drop_view_or_table(chunk_view_name, database)
                    sql = self._generate_values_sql(chunk, chunk_view_name, database, columns)
                    
                    # Display progress
                    logger.info(f"Creating part {i + 1} of {total_parts}: {chunk_view_name} ({len(chunk)} rows)")
                    
                    # Don't log SQL on error for split views (they're large)
                    self._execute_view_creation(sql, database, log_sql_on_error=False)
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

            # Verify the view is queryable
            logger.info("Verifying UNION view is queryable...")
            try:
                test_query = f"SELECT COUNT(*) FROM {database}.{union_view_name}"
                result = self.athena_helper.query(sql=test_query, database=database)
                logger.info(f"UNION view verified: {result[0][0]} total rows")
            except Exception as e:
                logger.warning(f"Could not verify UNION view: {e}")

            return True

        except Exception as e:
            logger.error("Failed to create UNION view: %s", str(e), exc_info=True)
            return False
    
    def _execute_view_creation(self, sql: str, database: str, log_sql_on_error: bool = True) -> None:
        """
        Execute view creation SQL using CID Athena helper.
        
        Args:
            sql: SQL statement to execute
            database: Database context for execution
            log_sql_on_error: Whether to log SQL in error messages (default True)
            
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
            # Extract just the error message without SQL
            error_msg = str(e)
            
            if log_sql_on_error:
                logger.error("Failed to execute view creation: %s", error_msg)
                # Only log SQL if it's reasonably sized
                if len(sql) < 5000:
                    logger.error("Query:\n%s", sql)
                else:
                    logger.debug("Query was too large to log (%d bytes)", len(sql.encode('utf-8')))
            else:
                logger.error("Failed to execute view creation: %s", error_msg)
                logger.debug("Query was suppressed (too large for logging)")
            
            # Re-raise with clean error message (without SQL)
            raise RuntimeError(f"View creation failed: {error_msg}") from None

    def write_complete_mapping(self, config: dict, df: pd.DataFrame,
                              database: str) -> dict:
        """
        Write all views: file source, config, and account map.

        This method orchestrates the complete view creation process:
        1. Cleanup old views with user confirmation
        2. Create file source view if needed
        3. Create config view
        4. Create account map view

        Args:
            config: Configuration dictionary
            df: Transformed account map DataFrame
            database: Target database name

        Returns:
            dict: Results containing created view names and status
        """
        from InquirerPy import inquirer

        results = {
            'file_source_view': None,
            'config_view': None,
            'account_map_view': None,
            'deleted_views': []
        }

        # Get view name from config or use default
        view_name = getattr(self, 'view_name', 'account_map')
        if hasattr(self, 'config') and 'general' in self.config:
            view_name = self.config.get('general', {}).get('view_name', 'account_map')

        # Step 1: Cleanup old views
        logger.info("Checking for existing views to cleanup...")
        old_views = self.identify_related_views(view_name, database)

        # If we're keeping the existing file source view, exclude it from deletion
        keep_file_source = config.get('file_source', {}).get('use_existing_view', False)
        if keep_file_source and old_views:
            file_source_view_name = config.get('metadata', {}).get('file_source_view', f"{view_name}_file_source")
            old_views = [
                v for v in old_views
                if not v['name'].startswith(file_source_view_name)
            ]

        if old_views:
            print(f"\nFound {len(old_views)} existing views:")
            for view_info in old_views:
                view_display = f"  - {view_info['name']}"
                if 'timestamp' in view_info:
                    view_display += f" (created: {view_info['timestamp']})"
                print(view_display)
            print()  # Empty line for readability

            confirm = inquirer.confirm(
                message=f"Delete {len(old_views)} existing view(s)?",
                default=True
            ).execute()

            if confirm:
                with spinner("Dropping existing views"):
                    for view_info in old_views:
                        view_name_to_delete = view_info['name']
                        try:
                            self._safe_drop_view_or_table(view_name_to_delete, database)
                            results['deleted_views'].append(view_name_to_delete)
                            logger.info(f"Deleted view: {view_name_to_delete}")
                        except Exception as e:
                            logger.warning(f"Failed to delete view {view_name_to_delete}: {e}")
            else:
                logger.info("Skipping view cleanup")

        # Step 2: Create file source view if needed
        if config.get('file_source') and not config['file_source'].get('use_existing_view'):
            file_view_name = config['metadata'].get('file_source_view', f"{view_name}_file_source")
            logger.info(f"Creating file source view: {file_view_name}")

            try:
                if 'data' not in config['file_source']:
                    raise RuntimeError("File source specified but no file data loaded")
                
                file_df = config['file_source']['data'].copy()
                account_col = config['file_source']['account_column']
                
                # Rename account column to account_id for JOIN compatibility
                if account_col != 'account_id':
                    file_df = file_df.rename(columns={account_col: 'account_id'})
                    logger.info(f"Renamed column '{account_col}' to 'account_id' for file source view")
                
                with spinner("Creating file source view"):
                    success = self.create_file_source_view(file_df, file_view_name, database)
                results['file_source_view'] = file_view_name if success else None
            except Exception as e:
                logger.error(f"Failed to create file source view: {e}")
                results['file_source_view'] = None
        elif config.get('file_source') and config['file_source'].get('use_existing_view'):
            # Using existing file source view - just record it
            file_view_name = config['metadata'].get('file_source_view', f"{view_name}_file_source")
            logger.info(f"Using existing file source view: {file_view_name}")
            results['file_source_view'] = file_view_name
        else:
            # No file source in config — check if stale file_source_view metadata needs cleanup
            stale_view = config.get('metadata', {}).get('file_source_view')
            has_file_dims = any(
                d['source_type'] == 'file' for d in config.get('taxonomy_dimensions', [])
            )
            if stale_view and not has_file_dims:
                logger.info(f"Cleaning up stale file source view: {stale_view}")
                try:
                    self._safe_drop_view_or_table(stale_view, database)
                    results['deleted_views'].append(stale_view)
                    logger.info(f"Dropped stale file source view: {stale_view}")
                except BaseException:
                    logger.debug(f"Stale file source view {stale_view} may not exist, skipping drop")
                # Remove stale metadata entries
                config['metadata'].pop('file_source_view', None)
                config.pop('file_source', None)

        # Step 3: Create config view
        logger.info(f"Creating config view: {view_name}_config")
        config_mgr = ConfigManager(self.athena_helper, view_name)
        with spinner("Creating configuration view"):
            success = config_mgr.save_to_view(config, database)
        results['config_view'] = f"{view_name}_config" if success else None

        # Step 4: Create account map view
        logger.info(f"Creating account map view: {view_name}")
        with spinner("Creating account map view"):
            success = self.create_account_map_view(config, df, view_name, database)
        results['account_map_view'] = view_name if success else None

        return results

    def identify_related_views(self, view_name: str, database: str) -> List[dict]:
        """
        Identify views matching account_map pattern for deletion.

        This method finds all views related to the specified view name that should be deleted:
        - Exact match: view_name
        - Part views: view_name_part1, view_name_part2, etc.
        - File source: view_name_file_source
        - File source parts: view_name_file_source_part1, etc.

        Excludes config views (they will be recreated separately):
        - Config view: view_name_config
        - Config parts: view_name_config_part1, etc.

        Args:
            view_name: Base view name to search for
            database: Database name

        Returns:
            List of dicts with 'name' and optional 'timestamp' keys (sorted alphabetically by name)
        """
        import re

        try:
            # Get all tables/views in the database
            all_tables = self.athena_helper.list_table_metadata(database)

            related_views = []
            for table in all_tables:
                name = table['Name']
                
                # Check if this view matches our patterns
                is_match = False
                
                # Explicitly exclude config views first (they will be recreated)
                if name == f"{view_name}_config":
                    is_match = False
                elif re.match(f"^{re.escape(view_name)}_config_part\\d+$", name):
                    is_match = False
                # Match exact name
                elif name == view_name:
                    is_match = True
                # Match name_partN pattern
                elif re.match(f"^{re.escape(view_name)}_part\\d+$", name):
                    is_match = True
                # Match name_file_source
                elif name == f"{view_name}_file_source":
                    is_match = True
                # Match name_file_source_partN pattern
                elif re.match(f"^{re.escape(view_name)}_file_source_part\\d+$", name):
                    is_match = True
                
                if is_match:
                    view_info = {'name': name}
                    # Add timestamp if available
                    if 'CreateTime' in table:
                        view_info['timestamp'] = table['CreateTime']
                    related_views.append(view_info)

            # Sort by name
            return sorted(related_views, key=lambda x: x['name'])

        except Exception as e:
            logger.error(f"Failed to identify related views: {e}")
            return []

    def create_file_source_view(self, df: pd.DataFrame, view_name: str,
                               database: str) -> bool:
        """
        Create view from file data using VALUES clause.

        Args:
            df: File data DataFrame
            view_name: Name for the file source view
            database: Target database name

        Returns:
            True if successful, False otherwise
        """
        try:
            view_names = self.create_view_from_values(df, view_name, database)

            if len(view_names) == 1:
                logger.info(f"Created file source view: {view_name}")
                return True
            else:
                # Multiple views created, need UNION view
                logger.info(f"Created {len(view_names)} file source parts, creating UNION view")
                success = self.create_union_view(view_names, view_name, database)
                return success

        except Exception as e:
            logger.error(f"Failed to create file source view: {e}")
            return False

    def create_account_map_view(self, config: dict, df: pd.DataFrame,
                               view_name: str, database: str) -> bool:
        """
        Create account map transformation view.

        This method creates the main account map view as a transformation view
        that queries the source table directly. If file sources are used, it
        creates a view that JOINs with the file source view.

        The transformed DataFrame is only used as a fallback if the SQL
        generation fails or exceeds size limits.

        Args:
            config: Configuration dictionary
            df: Transformed account map DataFrame (used as fallback only)
            view_name: Name for the account map view
            database: Target database name

        Returns:
            True if successful, False otherwise
        """
        try:
            # Always try transformation view approach first
            logger.info("Creating account map transformation view")
            sql = self._generate_account_map_transformation_sql(config, view_name, database)
            
            # Check size and split if needed
            if len(sql.encode('utf-8')) > self.MAX_SQL_SIZE:
                logger.info("Account map SQL exceeds Athena size limit, creating separate views due to size limits")
                view_names = self.create_view_from_values(df, view_name, database)
                if len(view_names) > 1:
                    return self.create_union_view(view_names, view_name, database)
                return True
            else:
                self._execute_view_creation(sql, database)
                return True

        except Exception as e:
            logger.error(f"Failed to create account map view: {e}")
            return False

    def _generate_account_map_transformation_sql(self, config: dict, view_name: str, database: str) -> str:
        """
        Generate SQL for account map transformation view.

        This creates a SELECT statement that transforms the source table
        according to the configured taxonomy dimensions, with support for:
        - Tag-based dimensions using json_extract_scalar
        - File-based dimensions with COALESCE for precedence
        - Joining with file source view when file is used

        Args:
            config: Configuration dictionary with metadata and taxonomy_dimensions
            view_name: Name for the view
            database: Target database name

        Returns:
            Complete CREATE VIEW SQL statement
        """
        # Extract metadata
        metadata = config.get('metadata', {})
        source_database = metadata.get('source_database', database)
        source_table = metadata.get('source_table')
        file_source_view = metadata.get('file_source_view')
        
        # Get taxonomy dimensions
        taxonomy_dimensions = config.get('taxonomy_dimensions', [])
        
        # Track dimension names to detect duplicates
        dimension_names_used = set()
        
        # Build SELECT clause with base columns
        select_parts = [
            'org.id AS account_id',
            'org.name AS account_name',
            'org.managementaccountid AS payer_id'
        ]
        
        # Add payer_name column if payer names are configured
        payer_names = config.get('payer_names', {})
        if payer_names:
            case_parts = []
            for pid, pname in payer_names.items():
                escaped_name = pname.replace("'", "''")
                case_parts.append(f"WHEN org.managementaccountid = '{pid}' THEN '{escaped_name}'")
            case_expr = "CASE\n                " + "\n                ".join(case_parts)
            case_expr += "\n                ELSE org.managementaccountid\n            END"
            select_parts.append(f"{case_expr} AS payer_name")
        
        # Add taxonomy dimension columns
        dimension_parts = []  # Collect as (output_name, sql_expr) for sorting
        for dimension in taxonomy_dimensions:
            dim_name = dimension['name']
            source_type = dimension['source_type']
            source_value = dimension['source_value']
            
            # Check for duplicate dimension names
            output_name = dim_name
            if dim_name in dimension_names_used:
                # Duplicate name - append suffix based on source type
                if source_type == 'tag':
                    output_name = f"{dim_name}_tag"
                elif source_type == 'file':
                    output_name = f"{dim_name}_file"
                elif source_type == 'name_split':
                    output_name = f"{dim_name}_split"
                logger.warning(f"Duplicate dimension name '{dim_name}' detected. Using '{output_name}' for {source_type} source.")
            
            dimension_names_used.add(output_name)
            
            if source_type == 'tag':
                tag_expr = f"""element_at(
                    filter(org.hierarchytags, x -> x.key = '{source_value}'),
                    1
                ).value"""
                dimension_parts.append((output_name, f"{tag_expr} AS {output_name}"))
                    
            elif source_type == 'file':
                if file_source_view:
                    dimension_parts.append((output_name, f"file.{source_value} AS {output_name}"))
                else:
                    logger.warning(f"File source dimension {dim_name} specified but no file source view")
                    dimension_parts.append((output_name, f"NULL AS {output_name}"))
                    
            elif source_type == 'name_split':
                if isinstance(source_value, dict):
                    separator = source_value.get('separator', '-')
                    index = source_value.get('index', 0)
                    split_expr = f"split_part(org.name, '{separator}', {index + 1})"
                    dimension_parts.append((output_name, f"{split_expr} AS {output_name}"))
                else:
                    logger.warning(f"Name split dimension {dim_name} has invalid source_value format")
                    dimension_parts.append((output_name, f"NULL AS {output_name}"))
        
        # Sort taxonomy dimensions alphabetically by output name
        dimension_parts.sort(key=lambda x: x[0].lower())
        select_parts.extend(expr for _, expr in dimension_parts)
        
        # Build FROM clause
        from_clause = f'FROM {source_database}.{source_table} org'
        
        # Add LEFT JOIN for file source if present
        if file_source_view:
            from_clause += f'\nLEFT JOIN {database}.{file_source_view} file ON org.id = file.account_id'
        
        # Build complete SQL
        select_clause = ',\n    '.join(select_parts)
        
        sql = f"""CREATE VIEW {database}.{view_name} AS
SELECT
    {select_clause}
{from_clause}"""
        
        return sql




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
            logger.debug(
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

