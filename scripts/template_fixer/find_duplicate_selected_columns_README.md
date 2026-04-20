# Find Duplicate Selected Columns

A tool to detect and fix duplicate entries in QuickSight dashboard YAML definition files.

## Problem

QuickSight dashboard deployments fail with error:
```
InvalidParameterValueException: Duplicate field in SelectedFieldsConfiguration
```

This occurs when YAML definition files contain duplicate `SelectedColumns` or `SelectedFields` entries.

## What It Does

Finds and removes duplicate `SelectedColumns` and `SelectedFields` entries in YAML files.

## Example of Problematic YAML

```yaml
SelectedFieldsConfiguration:
  SelectedColumns:
    - ColumnName: account_id
      DataSetIdentifier: my_dataset
    - ColumnName: account_id          # Duplicate!
      DataSetIdentifier: my_dataset
    - ColumnName: region
      DataSetIdentifier: my_dataset
  SelectedFields:
    - calculated_field_1
    - calculated_field_1              # Duplicate!
    - calculated_field_2
```

The script removes duplicate entries, keeping only the first occurrence.

## Usage

### Check for duplicates (dry run)
```bash
# Single file
python3 find_duplicate_selected_columns.py dashboard.yaml

# All YAML files in a folder
python3 find_duplicate_selected_columns.py ./dashboards/
```

### Fix duplicates with confirmation
```bash
# Single file - prompts for each fix
python3 find_duplicate_selected_columns.py dashboard.yaml --fix

# Folder - prompts for each fix
python3 find_duplicate_selected_columns.py ./dashboards/ --fix
```

### Fix duplicates automatically
```bash
# Single file - no prompts
python3 find_duplicate_selected_columns.py dashboard.yaml --fix --force

# Folder - no prompts
python3 find_duplicate_selected_columns.py ./dashboards/ --fix --force
```

## Options

- `<yaml_file_or_folder>` - Path to a YAML file or folder (scans recursively for .yaml/.yml files)
- `--fix` - Apply fixes (creates .backup files)
- `--force` - Skip confirmation prompts (requires --fix)

## Examples

```bash
# Check a single dashboard
python3 find_duplicate_selected_columns.py dashboards/cora/cora-definition.yaml

# Check all dashboards
python3 find_duplicate_selected_columns.py dashboards/

# Fix all dashboards automatically
python3 find_duplicate_selected_columns.py dashboards/ --fix --force
```

## Safety

- Creates `.backup` files before making changes
- Interactive mode (without `--force`) prompts for each fix
- Can quit anytime with 'q' in interactive mode
