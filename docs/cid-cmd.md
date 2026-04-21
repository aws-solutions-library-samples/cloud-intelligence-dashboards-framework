# CID-CMD — Cloud Intelligence Dashboards Command Line Tool

CID-CMD is a Python tool for managing QuickSight Dashboards and their dependencies (Datasets, DataSources, Athena Views, Glue Tables). It can also export dashboards to deployable artifacts for sharing across AWS accounts.

CID is also available as [CloudFormation templates](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html).

## Before You Start

1. Complete the prerequisites for the respective dashboard
2. [Specify a Query Result Location Using a Workgroup](https://docs.aws.amazon.com/athena/latest/ug/querying.html#query-results-specify-location-workgroup)
3. Activate QuickSight [Enterprise Edition](https://aws.amazon.com/premiumsupport/knowledge-center/quicksight-enterprise-account/)

## Installation

```bash
pip3 install --upgrade cid-cmd
```

Or launch [AWS CloudShell](https://console.aws.amazon.com/cloudshell/home) and install there.

## Syntax

```bash
cid-cmd [tool options] <command> [command options]
```

## Tool Options

These options apply to all commands and must be placed before the command name.

| Option | Description |
|---|---|
| `--profile` / `--profile_name` | AWS profile name to use |
| `--region_name` | AWS region |
| `--aws_access_key_id` | AWS access key ID |
| `--aws_secret_access_key` | AWS secret access key |
| `--aws_session_token` | AWS session token |
| `--log_filename` | Log file name (default: `cid.log`) |
| `-v` / `--verbose` | Increase log verbosity (use `-vv` for debug) |
| `-y` / `--yes` | Auto-confirm all yes/no prompts |

---

## Commands

### deploy

Deploy a Cloud Intelligence Dashboard.

```bash
cid-cmd deploy
```

| Option | Description |
|---|---|
| `--category TEXT` | Dashboard category (e.g., `foundational`, `advanced`). Not needed if `--dashboard-id` is provided |
| `--dashboard-id TEXT` | Dashboard ID (e.g., `cudos`, `cost_intelligence_dashboard`, `kpi_dashboard`, `ta-organizational-view`, `trends-dashboard`) |
| `--athena-database TEXT` | Athena database |
| `--athena-workgroup TEXT` | Athena workgroup |
| `--glue-data-catalog TEXT` | Glue data catalog (default: `AwsDataCatalog`) |
| `--cur-table-name TEXT` | CUR table name |
| `--quicksight-datasource-id TEXT` | QuickSight DataSource ID. Only Glue/Athena DataSources in healthy state can be used. If omitted, auto-discovered or created |
| `--quicksight-datasource-role-arn TEXT` | IAM Role for DataSource creation. Must have access to Athena and S3 |
| `--allow-buckets TEXT` | Comma-separated list of S3 bucket names to add to the default CID QuickSight role |
| `--quicksight-user TEXT` | QuickSight user |
| `--quicksight-group TEXT` | QuickSight group |
| `--dataset-{name}-id TEXT` | QuickSight dataset ID for a specific dataset |
| `--view-{name}-{param} TEXT` | Custom parameter for view creation (supports `{account_id}` variable) |
| `--account-map-source TEXT` | Account map source: `csv`, `dummy`, or `organization` |
| `--account-map-file TEXT` | CSV file path (when `csv` is selected as account map source) |
| `--on-drift (show\|override)` | Action on view/dataset drift. `show` (default) displays a diff, `override` replaces customizations |
| `--update (yes\|no)` | Update if elements are already installed (default: `no`) |
| `--resources TEXT` | CID resources YAML file or URL |
| `--catalog TEXT` | Comma-separated list of catalog files or URLs |
| `--theme TEXT` | QuickSight theme: `CLASSIC`, `MIDNIGHT`, `SEASIDE`, or `RAINIER` |
| `--currency TEXT` | Currency symbol: `USD`, `GBP`, `EUR`, `JPY`, `KRW`, `DKK`, `TWD`, `INR` |
| `--rls TEXT` | Row Level Security status: `CLEAR`, `ENABLED`, or `DISABLED` |
| `--rls-dataset-id TEXT` | ID of the RLS dataset |
| `--share-with-account (yes\|no)` | Make dashboard visible to other users in the same account |
| `--quicksight-delete-failed-datasource` | Delete datasource if creation failed |

### update

Update an existing dashboard. Optionally update all dependencies (Datasets and Athena Views).

```bash
# Update dashboard only
cid-cmd update

# Update dashboard and all dependencies (overrides customizations)
cid-cmd update --force --recursive
```

| Option | Description |
|---|---|
| `--dashboard-id TEXT` | QuickSight dashboard ID |
| `--force` / `--noforce` | Allow selecting up-to-date dashboards |
| `--recursive` / `--norecursive` | Recursively update all Datasets and Views |
| `--on-drift (show\|override)` | Action on view/dataset drift |
| `--theme TEXT` | QuickSight theme |
| `--currency TEXT` | Currency symbol |
| `--rls TEXT` | Row Level Security status |
| `--rls-dataset-id TEXT` | ID of the RLS dataset |

### status

Show the status of deployed dashboards.

```bash
cid-cmd status
cid-cmd status --dashboard-id cudos
```

| Option | Description |
|---|---|
| `--dashboard-id TEXT` | Show status for a specific dashboard |

### delete

Delete a dashboard and all dependencies unused by other CID-managed dashboards (including QuickSight datasets, Athena views, and tables).

```bash
cid-cmd delete
cid-cmd delete --dashboard-id cudos
```

| Option | Description |
|---|---|
| `--dashboard-id TEXT` | QuickSight dashboard ID |
| `--athena-database TEXT` | Athena database |

### export

Export a customized dashboard for sharing with another AWS account. Takes a QuickSight Analysis as input and generates all assets needed for deployment in another account.

```bash
# Export from account A
cid-cmd export

# Deploy in account B
cid-cmd deploy --resources ./mydashboard.yaml
```

| Option | Description |
|---|---|
| `--analysis-name TEXT` | Analysis name (not needed if `--analysis-id` is provided) |
| `--analysis-id TEXT` | Analysis ID (from the browser URL) |
| `--one-file (no\|yes)` | Generate a single file (default: `no`) |
| `--template-id TEXT` | Template ID |
| `--dashboard-id TEXT` | Target dashboard ID |
| `--template-version TEXT` | Version description (vX.Y.Z) |
| `--taxonomy TEXT` | Fields to keep as global filters |
| `--reader-account TEXT` | Account ID to share with (or `*` for all) |
| `--dashboard-export-method (definition\|template)` | Export method: pull JSON definition or create QuickSight Template |
| `--export-known-datasets (no\|yes)` | Include datasets already in resources file (default: `no`) |
| `--export-tables (no\|yes)` | Include tables in export (default: `no`, views only) |
| `--category TEXT` | Dashboard category (default: `Custom`) |
| `--output TEXT` | Output filename (.yaml) |

### share

Share QuickSight resources (Dashboard, Datasets, DataSource) with users, groups, or the entire account.

```bash
cid-cmd share
```

| Option | Description |
|---|---|
| `--dashboard-id TEXT` | QuickSight dashboard ID |
| `--share-method (folder\|user\|account)` | Sharing method |
| `--folder-method (new\|existing)` | Create new folder or use existing |
| `--folder-id TEXT` | Existing QuickSight folder ID |
| `--folder-name TEXT` | New QuickSight folder name |
| `--quicksight-user TEXT` | QuickSight user |
| `--quicksight-group TEXT` | QuickSight group |

### open

Open a dashboard in the browser.

```bash
cid-cmd open
cid-cmd open --dashboard-id cudos
```

| Option | Description |
|---|---|
| `--dashboard-id TEXT` | QuickSight dashboard ID |

### map

Create an `account_map` Athena view that enriches AWS account data with custom taxonomy dimensions (business unit, environment, cost center, etc.). These dimensions can then be used in Cloud Intelligence Dashboards for grouping, filtering, and reporting. Supports using tags and splitting account name by separator as well as supplying a file with taxonomy dimensions.

```bash
# Interactive mode (default) — walks you through configuration
cid-cmd map

# Provide a file with additional taxonomy data
cid-cmd map --file accounts.csv

# Legacy mode — simple account_id/account_name mapping from organization_data
cid-cmd map --simple

# Custom output view name
cid-cmd map --view-name my_account_map
```

| Option | Description |
|---|---|
| `--view-name TEXT` | Output view name (default: `account_map`) |
| `--simple` | Use simple account mapping (legacy mode) — creates a basic view with just `account_id` and `account_name` |
| `--file PATH` | Path to CSV/Excel/JSON file for file-based taxonomy dimensions |
| `--database TEXT` | Source database containing `organization_data` (skips auto-discovery) |

#### How It Works

The command follows a six-phase workflow:

1. **Discovery** — auto-detects the `organization_data` table (from AWS Organizations data collection) and the target Athena database
2. **Configuration** — prompts you to select data sources and define taxonomy dimensions (or reuses a saved configuration)
3. **Data Loading** — reads organization data from Athena and optionally loads an external file
4. **Transformation** — applies taxonomy rules to produce the enriched account map
5. **Preview** — shows a sample of the output and the generated SQL for confirmation
6. **Write** — creates the Athena views (`account_map`, `account_map_config`, and optionally `account_map_file_source`)

#### Taxonomy Dimension Sources

During interactive configuration you choose one or more data sources for your taxonomy dimensions:

**Tags from source table** — Extracts values from the `hierarchytags` column in `organization_data`. Each selected tag key becomes a column in the output view. For example, if your accounts are tagged with `Environment=Production` and `CostCenter=Engineering`, selecting those tag keys produces `environment` and `cost_center` columns.

**Additional file (`--file`)** — Joins columns from a CSV/Excel/JSON file by account ID. The file must contain an account ID column; all other selected columns become taxonomy dimensions. Example file:
```csv
account_id,business_unit,team
123456789012,Retail,Frontend
234567890123,Platform,Data
```

**Split account name** — Extracts dimensions by splitting the `account_name` string on a separator character. You specify the separator and the positional index to extract. For example, for account name `aws-retail-prod`, splitting by `-` at index 1 yields `retail`, and at index 2 yields `prod`.

#### Configuration Persistence

The command saves its configuration as an Athena view (`<view_name>_config`). On subsequent runs it detects the existing config and offers to reuse it, so you don't have to reconfigure every time.

#### Views Created

| View | Purpose |
|---|---|
| `account_map` | The main enriched account mapping view used by dashboards |
| `account_map_config` | Stores the mapping configuration for reuse |
| `account_map_file_source` | (Only when `--file` is used) Stores the file data as an Athena view for joins |

#### Map Prerequisites

- An `organization_data` table in Athena (typically created by the CID data collection CFN stack)
- Athena workgroup with a configured query result location
- Appropriate IAM permissions for Athena and Glue operations

#### Map Examples

Create an account map using AWS Organization tags:
```bash
cid-cmd map
# → Select "Tags from source table"
# → Pick tag keys like Environment, CostCenter, Team
# → Preview and confirm
```

Create an account map enriched with data from a spreadsheet:
```bash
cid-cmd map --file ~/Downloads/account_taxonomy.xlsx
# → Select "Additional file" and/or "Tags from source table"
# → Pick columns from the file to use as dimensions
# → Preview and confirm
```

Re-run with saved configuration (no prompts):
```bash
cid-cmd map -y
```

### csv2view

Generate a SQL file for an Athena View from a CSV file. Mind the [Athena service limit for query size](https://docs.aws.amazon.com/athena/latest/ug/service-limits.html#service-limits-query-string-length).

```bash
cid-cmd csv2view --input my_mapping.csv --name my_mapping
```

| Option | Description |
|---|---|
| `--input TEXT` | CSV file path |
| `--name TEXT` | Athena View name |

### init-qs

One-time action to initialize Amazon QuickSight Enterprise Edition.

```bash
cid-cmd init-qs
```

| Option | Description |
|---|---|
| `--enable-quicksight-enterprise (yes\|no)` | Confirm QuickSight activation |
| `--account-name TEXT` | Unique QuickSight account name (unique across all AWS users) |
| `--notification-email TEXT` | Email for QuickSight notifications |

### create-cur-table

One-time action to initialize an Athena table and Crawler from S3 with CUR data. Currently only CUR1 is supported.

```bash
cid-cmd create-cur-table
```

| Option | Description |
|---|---|
| `--view-cur-location TEXT` | S3 path with CUR data (e.g., `s3://BUCKET/cur` or `s3://BUCKET/prefix/cur_name/cur_name`) |
| `--crawler-role TEXT` | Name or ARN of the crawler role |

### create-cur-proxy

Create a CUR proxy — an Athena view that transforms CUR1 to CUR2 format or vice versa. Cost Allocation Tags and Cost Categories are not included by default; add them with `--fields`.

```bash
# CUR2 proxy from CUR1 data
cid-cmd create-cur-proxy --cur-version 2 --cur-table-name mycur1 \
  --athena-workgroup primary \
  --fields "resource_tags['user_cost_center'],resource_tags['user_owner']"

# CUR1 proxy from CUR2 data
cid-cmd create-cur-proxy --cur-version 1 --cur-table-name mycur2 \
  --athena-workgroup primary \
  --fields "resource_tags_user_cost_center,resource_tags_user_owner"
```

| Option | Description |
|---|---|
| `--cur-version (1\|2)` | Target CUR version |
| `--fields TEXT` | Comma-separated list of additional CUR fields |
| `--cur-table-name TEXT` | CUR table name |
| `--cur-database TEXT` | Athena database of CUR |
| `--athena-database TEXT` | Athena database to create proxy in |

### cleanup

Delete unused QuickSight datasets and Athena views that are no longer referenced by any dashboard.

```bash
cid-cmd cleanup
```

### teardown

Delete all CID-managed assets. This removes all dashboards, datasets, datasources, and related resources.

```bash
cid-cmd teardown
```

> **Warning:** This is a destructive operation that removes everything created by CID.

---

## Common Parameters

These parameters can be passed to most commands as extra `--key value` arguments:

| Parameter | Description |
|---|---|
| `--dashboard-id TEXT` | QuickSight Dashboard ID |
| `--athena-database TEXT` | Athena database |
| `--athena-workgroup TEXT` | Athena workgroup |
| `--glue-data-catalog TEXT` | Glue data catalog (default: `AwsDataCatalog`) |
| `--cur-table-name TEXT` | CUR table name |
| `--quicksight-datasource-id TEXT` | QuickSight DataSource ID |
| `--quicksight-user TEXT` | QuickSight user |
| `--dataset-{name}-id TEXT` | QuickSight dataset ID for a specific dataset |
| `--view-{name}-{param} TEXT` | Custom parameter for view creation (supports `{account_id}` variable) |
| `--account-map-source TEXT` | Account map source: `csv`, `dummy`, or `organization` |
| `--account-map-file TEXT` | CSV file path for account mapping |
| `--resources TEXT` | CID resources YAML file or URL |
| `--share-with-account (yes\|no)` | Share dashboard with all users in the account |

---

## Troubleshooting

Run any command in debug mode to produce a detailed log file:

```bash
cid-cmd -vv <command>
```

This creates a `cid.log` file in the current directory. Inspect it for sensitive information before sharing.

Report issues at the [GitHub repository](https://github.com/aws-samples/aws-cudos-framework-deployment/issues/new).
