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
| `--file PATH` | Path to a CSV file containing additional taxonomy columns to join by account ID |
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

**Account/OU tags** — Extracts values from the `hierarchytags` column in `organization_data`. Each selected tag key becomes a column in the output view. For example, if your accounts are tagged with `Environment=Production` and `CostCenter=Engineering`, selecting those tag keys produces `environment` and `cost_center` columns.

**Additional file (`--file`)** — Joins columns from a CSV/Excel/JSON file by account ID. The file must contain an account ID column; all other selected columns become taxonomy dimensions. Example file:
```csv
account_id,business_unit,team
123456789012,Retail,Frontend
234567890123,Platform,Data
```

**OU name** — Extracts the organizational unit name at a given level in the account's OU hierarchy. The tool discovers available levels from your data and shows sample values. For example, if an account's hierarchy is `ROOT > Pegasus > Team-A`, selecting level 2 produces `Pegasus`. Generates SQL like `TRY(org.hierarchy[2].name)`.

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
# → Select "Account/OU tags"
# → Pick tag keys like Environment, CostCenter, Team
# → Preview and confirm
```

Create an account map enriched with data from a spreadsheet:
```bash
cid-cmd map --file ~/Downloads/account_taxonomy.xlsx
# → Select "Additional file" and/or "Account/OU tags"
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

### create-agent

Create a Quick Agent (an AI advisor) and its knowledge Space over CID dashboards that are **already deployed**. An agent's knowledge layer is a **Space** — a Quick container that holds references to your already-deployed dashboards (and, optionally, datasets and pre-existing knowledge bases). This command never deploys dashboards and never provisions the data layer (CUR, Data Exports, Data Collection); when required dashboards are missing it points to the existing CID/CUDOS and Data Collection deployment guidance. When the agent is already deployed, the command shows what differs from the catalog and asks whether to update instead — pass `--update yes` to skip the prompt, or use [update-agent](#update-agent).

```bash
# Interactive — shows a category-grouped picker of catalog agents
cid-cmd create-agent

# Non-interactive with a bring-your-own Space
cid-cmd create-agent --agent-id finops --space 'My Team Space' -y
```

| Option | Description |
|---|---|
| `--agent-id TEXT` | Agent id from the catalog (a category-grouped picker is shown when omitted) |
| `--space TEXT` | Name of a Space to use instead of the per-agent default Space (bring-your-own Space) |
| `--cleanup-space` | Remove only CID-managed Space dashboard resources that are no longer referenced by any agent's dependencies |
| `--update (yes\|no)` | When the agent is already deployed, update it without prompting ('no' exits with guidance) |

#### Agent Prerequisites

- An active **QuickSight Enterprise** subscription. The check is read-only — `create-agent` never activates or modifies a subscription. Missing subscription stops the command with instructions to enable Enterprise.
- The caller must be a **registered QuickSight user** with the **Author Pro** (or Admin Pro) role, since creating Quick Agents requires the Author Pro entitlement.
- The Quick Suite generative-AI operations must be available in your region and partition, with an AWS SDK new enough to carry them (`boto3 >= 1.43.0`, installed automatically with `cid-cmd`).

#### The prerequisite-only model

`create-agent` **never deploys dashboards** and **never provisions the data layer** (CUR, Data Exports, or Data Collection). It works only with prerequisites that already exist: it builds a Space over dashboards that are **already deployed** in your account and publishes an Agent on top of that Space.

This means:

- If a dashboard an agent needs is missing, `create-agent` will not install it. It points you to the existing deployment guidance instead (see below).
- Your CUR, Data Exports, and Data Collection setup is never touched, read-only checks aside.
- Re-running `create-agent` is safe: when the agent is already deployed, it shows what differs from the catalog and asks before updating anything; Space resources are added additively and de-duplicated.

#### Required vs Optional dashboards

Each agent declares two kinds of dashboard dependencies:

- **Required dashboards** (`dependsOn.dashboards`) — foundational dashboards the agent is designed around, such as CUDOS, Cost Intelligence, and KPI.
- **Optional dashboards** (`dependsOn.optionalDashboards`) — advanced dashboards that enhance the agent's capabilities and depend on the separate Data Collection deployment, such as the Trends dashboard.

At run time, `create-agent` pre-flights which dependency dashboards are actually deployed (a read-only check) and then:

- **Zero dashboards present** — the command stops with an error and creates nothing. Deploy the foundational dashboards first, following the [Cloud Intelligence Dashboards (CID/CUDOS) deployment guidance](https://catalog.workshops.aws/awscid/en-US).
- **A Required dashboard is missing** — you get a warning naming the dashboard and pointing to the [CID/CUDOS deployment guidance](https://catalog.workshops.aws/awscid/en-US); the command proceeds with the dashboards that are present.
- **An Optional dashboard is missing** — you get a warning that the related capabilities require that Advanced dashboard, which depends on the separate [CID Data Collection deployment](https://catalog.workshops.aws/awscid/en-US/data-collection); the command proceeds with the dashboards that are present.

Agents may also declare dataset knowledge (`dependsOn.datasets`). Present datasets are attached to the Space; missing datasets produce a warning and are skipped — datasets are never created or deployed by this command.

#### Bring-your-own Space (`--space`)

By default each agent uses the Space declared in its catalog definition (the launch-library agents share `cid-dashboards-space`). Pass `--space NAME` to use a Space you name instead:

```bash
cid-cmd create-agent --agent-id finops --space 'My Team Space'
```

- **The name matches one existing Space** — that Space is reused **additively**: only this agent's dashboards are added. Pre-existing Space resources, name, description, tags, and permissions are left untouched. This applies even to Spaces that were not created by CID.
- **The name matches multiple Spaces** — the command stops with an error listing the matching space ids. Rename Spaces so the name is unique, then re-run.
- **The name matches no Space** — a new Space is created with an id derived deterministically from the name.

Multiple agents can share one Space: resources are added additively and de-duplicated by ARN, so agents never clobber each other's dashboards.

#### Removing stale Space resources (`--cleanup-space`)

Space updates are additive by default: `create-agent` never removes anything from a Space. Over time a shared Space can accumulate dashboard references that no agent uses anymore. The opt-in flag cleans those up:

```bash
cid-cmd create-agent --agent-id finops --cleanup-space
```

Removal is strictly scoped. A dashboard resource is removed from the Space only when it is **both**:

1. **CID-managed** — it belongs to the CID dashboard catalog, and
2. **unreferenced** — no catalog agent's dependencies reference it anymore.

Resources added to the Space by other tools or by hand are never removed. The same flag is available on `delete-agent` to tidy a retained shared Space after an agent is removed.

#### The launch library

Three agents ship with the core catalog (run `cid-cmd list-agents` to see them):

| Agent id | Name | Focus |
|---|---|---|
| `finops` | CID FinOps Advisor | Cost optimization and anomaly triage over CUDOS, CID, and KPI |
| `operations` | CID Operations Advisor | Operational health and incident triage signals |
| `security` | CID Security Advisor | Security finding triage over Trusted Advisor data |

Want to add your own agent? See the [zero-Python contribution guide](agents-contributing.md).

### update-agent

Update a deployed CID-managed Quick Agent to match the catalog — for example after a persona or starter-prompt change. The command shows which catalog-managed fields drifted (persona fields, starter prompts, welcome message, name, action connectors) and asks for confirmation before overriding them, since the update also overrides any customizations made in the Quick console to those fields. With `-y` or in unattended mode the update applies without a prompt. If nothing drifted, the agent is reported up to date and no agent-mutating call is made (missing Space resources are still added). The agent must exist — run [create-agent](#create-agent) first.

```bash
cid-cmd update-agent --agent-id finops
```

| Option | Description |
|---|---|
| `--agent-id TEXT` | Agent id from the catalog (a category-grouped picker is shown when omitted) |
| `--space TEXT` | Name of a Space to use instead of the per-agent default Space (bring-your-own Space) |
| `--cleanup-space` | Remove only CID-managed Space dashboard resources that are no longer referenced by any agent's dependencies |
| `--sync-spaces` | Exactly synchronize the agent Space links with the catalog (removes Spaces the catalog does not carry) |
| `--repair` | Detach and re-attach the agent Spaces to rewrite the links (recovers an agent whose Space shows as unavailable although it describes as healthy) |

#### Additive Space links (`--sync-spaces`)

By default the update reconciles the agent's Space links **additively**: missing catalog Spaces are attached, and Spaces you attached outside the catalog are never detached. Pass `--sync-spaces` to make the links match the catalog exactly, removing any Space the catalog does not carry.

#### Repairing Space links (`--repair`)

An agent can show its Space as unavailable (and fail chat) even though it describes as healthy — the stored link is broken in a way no read API surfaces. The opt-in flag rewrites the links:

```bash
cid-cmd update-agent --agent-id finops --repair
```

The repair detaches every attached Space, waits for the agent to settle, then re-attaches the same Spaces. Two separate calls, because the API rejects the same ARN in both the add and remove lists of one call, and an add alone can leave the broken link in place.

### list-agents

List catalog agents grouped by category with live deployment status. Deployed agents are marked with a check indicator; entries in category `Deprecated` are hidden. Each entry also lists the agent's required and optional dependency dashboards with their deployment state (✓/✗). Deployment status is read live from the account (no local state file).

```bash
cid-cmd list-agents
```

### delete-agent

Delete a CID-managed Quick Agent. This command never deletes any dashboard.

```bash
cid-cmd delete-agent --agent-id finops
```

| Option | Description |
|---|---|
| `--agent-id TEXT` | Agent id to delete |
| `--delete-space` | Also delete the Agent's Space when no other CID-managed agent depends on it |
| `--cleanup-space` | Remove only CID-managed Space dashboard resources that are no longer referenced by any agent's dependencies |

- Deletion asks for confirmation (default `no`; pass `-y` to confirm non-interactively).
- A missing target is treated as success.
- Agents that were not created by CID are refused — `cid-cmd` never deletes resources it does not manage.
- **No dashboard is ever deleted** as part of deleting an agent.
- `--delete-space` also deletes the agent's Space, but only when no other CID-managed agent depends on it; otherwise the Space is retained and the dependency reported.

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
