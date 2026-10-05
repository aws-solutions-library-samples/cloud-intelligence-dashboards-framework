# Dashboard Resource Standards

Conventions for dashboard resource files (`dashboards/<name>/<name>.yaml`) deployed with `cid-cmd`. Follow them when creating or reviewing a dashboard. Section 8 is a review checklist.

---

## 1. Account names: use `account_map`, joined at dataset level

Every dashboard that shows account names gets them from the shared `account_map` view. Don't join account names in Athena view SQL, and don't read `organization_data` directly.

### 1.1 Why

- **One source of truth.** `cid-cmd` creates `account_map` once per Athena database and every dashboard reuses it, so names match across CUDOS, Health, Graviton, and so on.
- **No hard dependency on the Org Data module.** If `account_map` is missing, `cid-cmd` builds it from the best available source: it auto-discovers account metadata tables such as `organization_data` (Data Collection), or falls back to a one-time AWS Organizations listing (`organization`), a CSV file (`csv`), or "dummy" data from CUR with IDs only (`dummy`). Customers can choose with `--account-map-source` and `--account-map-file`. Reading `organization_data` directly makes view creation fail for single-account setups, accounts without AWS Organizations access, and deployments without the Org Data module.
- **No lost rows.** A `LEFT` join keeps every fact row even when an account has no name (new, closed, or outside the Organization). An `INNER` join silently drops those rows and understates totals.

### 1.2 Pattern

The view returns only the account ID. The dataset adds `account_map` as a second physical table, renames its key, and `LEFT`-joins it. Declare `account_map` in the dataset's `dependsOn.views` so `cid-cmd` creates it when it's missing.

```yaml
datasets:
  my_dataset:
    data:
      PhysicalTableMap:
        my-view-table:
          RelationalTable:
            DataSourceArn: ${athena_datasource_arn}
            Catalog: AwsDataCatalog
            Schema: ${athena_database_name}
            Name: my_view
            InputColumns:
            - Name: account_id
              Type: STRING
            # ... other view columns, no account_name here
        account-map-table:
          RelationalTable:
            DataSourceArn: ${athena_datasource_arn}
            Catalog: AwsDataCatalog
            Schema: ${athena_database_name}
            Name: account_map
            InputColumns:
            - Name: account_id
              Type: STRING
            - Name: account_name
              Type: STRING
      LogicalTableMap:
        my-view-logical:
          Alias: my_view
          Source:
            PhysicalTableId: my-view-table
        account-map-logical:
          Alias: account_map
          DataTransforms:
          - RenameColumnOperation:
              ColumnName: account_id
              NewColumnName: account_id[account_map]
          Source:
            PhysicalTableId: account-map-table
        my-joined-logical:
          Alias: my_dataset
          Source:
            JoinInstruction:
              LeftOperand: my-view-logical
              RightOperand: account-map-logical
              Type: LEFT
              OnClause: '{account_id} = {account_id[account_map]}'
    dependsOn:
      views:
      - my_view
      - account_map
```

Reference implementations: `dashboards/health-events/health-events.yaml`, `dashboards/graviton-savings-dashboard/graviton_savings_dashboard.yaml`, `dashboards/extended-support-cost-projection/extended-support-cost-projection.yaml`.

### 1.3 Details

- `account_map` always provides `account_id` and `account_name`. Some sources add `parent_account_id`/`parent_account_name` and taxonomy columns, so don't assume they exist.
- If the view's account column has a different name (for example `accountid` or `linked_account_id`), adjust only the `OnClause`: `'{accountid} = {account_id[account_map]}'`.
- To keep existing visuals and calculated fields working, rename `account_name` in the `account_map` logical table to the name the analysis already uses (for example `accountname`) with another `RenameColumnOperation`.
- Show "name (id)" with a calculated field rather than in SQL, for example `ifelse(strlen({account_name}) > 0, concat({account_name}, " (", {account_id}, ")"), {account_id})`.
- Exception: Row Level Security (`dashboards/rls`) reads `organization_data` by design.

---

## 2. Placeholders: never hardcode database or table names

All database and table references in view SQL and datasets use placeholders:

| Placeholder | Meaning |
|---|---|
| `${athena_database_name}` | Database where the dashboard creates its views (selected at deploy time) |
| `${cur2_database}` / `${cur2_table_name}` | CUR 2.0 database and table |
| `${cur_database}` / `${cur_table_name}` | Legacy CUR (CUR1) |
| `${data_collection_database_name}` | CID Data Collection database (resolved by a `parameters` lookup, see section 4) |
| `${athena_datasource_arn}` | QuickSight data source |

Don't write literals such as `optimization_data.`, `cid_cur.`, or `cur2.data`, even for objects that "always" exist. Customers rename databases, and literals break multi-database and multi-payer setups.

---

## 3. Declare every dependency in `dependsOn`

`cid-cmd` only resolves placeholders and creates prerequisites that are declared.

- **CUR.** Any view that reads `${cur2_database}`/`${cur2_table_name}` must declare `dependsOn: { cur2: true }`. Use `cur: true` / `cur1: true` for legacy CUR. Without it, `cid-cmd` doesn't initialize the CUR and the placeholders resolve to `None`, producing `FROM "None"."None"` and the Athena error `Schema 'none' does not exist`.
- **Views.** A view that selects from another view lists it in `dependsOn.views`. A dataset lists the views it reads, including `account_map` (section 1).
- **Prefer reading CUR directly** over stacking on shared helper views (`summary_view`, `resource_view`, `cur1_proxy`, …) owned by other dashboards. Those views can be stale or missing in the target database, and a schema change in CUR (for example a column becoming `double`) makes them invalid, which breaks every view built on top of them.

---

## 4. Prerequisite checks for Data Collection tables

Views that read Data Collection tables resolve `${data_collection_database_name}` with a `parameters` lookup. The lookup is also the prerequisite check, so it must cover **every** Data Collection table the view reads, not just one:

```yaml
views:
  my_view:
    parameters:
      data_collection_database_name:
        type: athena
        query: SELECT DISTINCT table_schema FROM information_schema.columns WHERE table_name = 'inventory_rds_db_instances_data'
        error: "Prerequisites not found. Enable the Inventory module in CID Data Collection."
```

- If a view also reads tables from other modules (for example Reference or a service module), check those too, and name the module to enable in the `error` message. Otherwise customers get a raw `TABLE_NOT_FOUND` instead of an actionable message.
- Data Collection table schemas: keep variable or nested fields as `string` and parse them in the view with `json_parse` / `CAST(... AS ARRAY(ROW(...)))` / `UNNEST`. Guard against empty values (`taglist IS NULL OR taglist = '[]'`) before parsing.

---

## 5. View SQL

- Start every view with `CREATE OR REPLACE VIEW "${athena_database_name}".<view_name> AS`.
- Prefer columns that are stable across CUR versions and pricing models. For amortized cost, follow the existing CID formula (`SavingsPlanCoveredUsage` → `savings_plan_savings_plan_effective_cost`, `DiscountedUsage` → `reservation_effective_cost`, otherwise `line_item_unblended_cost`).
- Recommendation logic must be driven by the computed dollar impact, not by a rule-of-thumb percentage alone. Show the percentage as context.
- Avoid `INNER JOIN` between fact data and enrichment or lookup data unless dropping unmatched rows is intended. Use `LEFT JOIN`.

---

## 6. Datasets

- `ImportMode: SPICE` with `schedules: [default]` unless the dashboard has a documented reason for direct query.
- Keep `InputColumns` and `ProjectedColumns` in sync with the view's output columns and types.
- Use stable, readable `DataSetId` values. They're referenced by the definition file and by `cid-cmd update`.

---

## 7. Dashboard metadata

- `dashboardId`: short, lowercase, stable (it becomes the URL and the `--dashboard-id` value).
- `category`: one of the categories used in the catalog and the documentation (`Foundational`, `Advanced`, `Additional`, `Security`), written in that case. Don't use `Custom` for dashboards in the catalog.
- `version`: a real semantic version (for example `v1.0.0`) matching the changelog. `cid-cmd update` compares it with the deployed version, and `v0.0.0` makes an update look like a downgrade.
- Add the dashboard to `dashboards/catalog.yaml` and a changelog under `changes/`.

---

## 8. Review checklist

- [ ] **Account names** come from `account_map`, joined at dataset level with a `LEFT` join and declared in `dependsOn.views`. No `organization_data` or `account_map` joins in view SQL (section 1).
- [ ] **No hardcoded databases or tables**: only placeholders (section 2).
- [ ] **`dependsOn`** declares `cur2`/`cur1` for every CUR-reading view, and all view and dataset dependencies (section 3).
- [ ] **No dependencies on other dashboards' helper views**: read CUR directly (section 3).
- [ ] **Prerequisite checks** cover every Data Collection table, with errors naming the module to enable (section 4).
- [ ] **No unintended `INNER JOIN`s** that drop fact rows (section 5).
- [ ] **Metadata**: `category`, a real `version`, catalog entry, changelog (section 7).
- [ ] **Deployed and tested** with `cid-cmd deploy --resources <file>`, then `cid-cmd update --resources <file> --force --recursive`. Confirm every view returns data and every dataset ingests into SPICE.
