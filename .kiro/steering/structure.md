# Project Structure

## Root Directory
- `README.md` - Main project documentation
- `CID-CMD.md` - Command-line tool documentation
- `pyproject.toml` - Python project configuration
- `setup.cfg` - Package metadata and dependencies
- `requirements.txt` - Python dependencies
- `LICENSE` - MIT-0 license
- `CONTRIBUTING.md` - Contribution guidelines

## Core Package (`cid/`)
- `cli.py` - Main command-line interface entry point
- `base.py` - Base classes and core functionality
- `common.py` - Shared utilities and constants
- `utils.py` - Helper functions
- `logger.py` - Logging configuration
- `plugin.py` - Plugin system implementation
- `exceptions.py` - Custom exception classes
- `export.py` - Dashboard export functionality
- `_version.py` - Version information

### Subpackages
- `cid/builtin/` - Built-in plugins and core functionality
- `cid/commands/` - CLI command implementations
- `cid/helpers/` - Helper modules and utilities
- `cid/test/` - Unit tests

## Dashboards (`dashboards/`)
- `catalog.yaml` - Master catalog of all available dashboards
- Individual dashboard folders (e.g., `cudos/`, `cost-intelligence/`, `kpi_dashboard/`)
- Each dashboard contains:
  - YAML definition files
  - SQL queries for Athena views
  - JSON metadata
  - Documentation

## CloudFormation Templates (`cfn-templates/`)
- `cid-cfn.yml` - Main CloudFormation template
- `cid-admin-policies.yaml` - IAM policies for CID administrators
- `cid-plugin.yml` - Plugin-specific template
- `cid-lakeformation-prerequisite.yaml` - Lake Formation setup
- `tests/` - CloudFormation template tests

## Other Directories
- `assets/` - Images, diagrams, and static resources
- `docs/` - Additional documentation
- `changes/` - Changelog files
- `terraform/` - Terraform infrastructure code
- `legacy-terraform/` - Legacy Terraform configurations
- `sandbox/` - Development and testing environment

## Configuration Files
- `.gitignore` - Git ignore patterns
- `.pylintrc` - Python linting configuration
- `.bandit` - Security scanning configuration
- `MANIFEST.in` - Package manifest for distribution

## Naming Conventions
- Dashboard folders use lowercase with hyphens (e.g., `cost-intelligence`)
- Python modules use lowercase with underscores
- YAML files use descriptive names matching their purpose
- CloudFormation templates use `cid-` prefix