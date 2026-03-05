# Feature Requirements: Dynamic FOCUS Consolidation View

## Feature Name
focus-consolidation-dynamic-view

## Overview
Enable cid-cmd to dynamically discover and consolidate FOCUS tables from multiple cloud providers (AWS, Azure, GCP, OCI) into a single `focus_consolidation_view` that automatically updates during deployment and `--recursive` updates. Additionally, update all downstream views and datasets to the FOCUS 1.2 schema with new columns and improved tag discovery queries.

## Problem Statement
Currently, `focus_consolidation_view` has two critical issues:
1. It uses `CREATE VIEW` instead of `CREATE OR REPLACE VIEW`, causing it to be skipped during `--recursive` updates
2. It's a static SQL file that requires manual editing to add/remove cloud provider FOCUS tables

This creates maintenance burden for customers who need to manually update the view when adding new cloud providers or when FOCUS schema versions differ across providers.

## User Stories

### 1. As a customer with multiple cloud providers
**I want** cid-cmd to automatically discover all my FOCUS tables across AWS, Azure, GCP, and OCI
**So that** I don't have to manually edit SQL files to consolidate my multi-cloud billing data

**Acceptance Criteria:**
1.1. cid-cmd discovers FOCUS tables across all Athena databases during deployment
1.2. Discovery uses FOCUS 1.0 minimum required columns (21 columns) to identify valid FOCUS tables
1.3. Discovery works regardless of database naming conventions
1.4. Discovery excludes non-FOCUS tables even if they have similar column names
1.5. Discovery excludes non-table objects (views, etc.) by checking `TableType == 'EXTERNAL_TABLE'`

### 2. As a customer with mixed FOCUS versions
**I want** the consolidation view to handle FOCUS 1.0, 1.1, and 1.2 tables seamlessly
**So that** I can use the FOCUS dashboard even when different cloud providers support different FOCUS versions

**Acceptance Criteria:**
2.1. Generated view includes all FOCUS 1.2 columns (58 columns)
2.2. Missing columns in source tables are filled with typed NULL values
2.3. Column types are cast to match FOCUS 1.2 specification when needed, using a type compatibility system
2.4. Special handling for `billing_period` column (partition vs computed)
2.5. Array-type source columns that map to scalar targets produce NULL instead of invalid CAST

### 3. As a customer running `--recursive` updates
**I want** `focus_consolidation_view` to be updated automatically
**So that** new FOCUS tables are included without manual intervention

**Acceptance Criteria:**
3.1. View uses `CREATE OR REPLACE VIEW` syntax
3.2. View is regenerated during `--recursive` updates
3.3. View regeneration discovers any newly added FOCUS tables
3.4. Existing data and dependent views continue to work after update

### 4. As a customer selecting which tables to include
**I want** to choose which discovered FOCUS tables are included in the consolidation view
**So that** I can exclude test or irrelevant tables

**Acceptance Criteria:**
4.1. In interactive mode (CLI), user is prompted to select from discovered tables
4.2. In non-interactive mode (CFN/Lambda), all discovered tables are auto-selected unless `focus-tables` parameter is explicitly set
4.3. Table labels support both quoted (`"database"."table"`) and unquoted (`database.table`) formats
4.4. Unmatched selections produce a warning and are skipped

### 5. As a customer updating an existing view
**I want** to see a diff of changes before the view is updated
**So that** I can review and approve changes before they take effect

**Acceptance Criteria:**
5.1. In interactive mode, a diff is shown between existing and proposed view SQL
5.2. User can choose: retry diff, proceed and override, keep existing, or exit
5.3. If view is already up to date (no diff), skip execution and report "No need to update"
5.4. In non-interactive mode, proceed automatically

### 6. As a developer maintaining cid-cmd
**I want** the FOCUS schema definition to be centralized and maintainable
**So that** future FOCUS specification updates can be implemented easily

**Acceptance Criteria:**
6.1. FOCUS 1.2 column schema is defined in `focus.yaml` under `focus_consolidation_view.columns` as the single source of truth
6.2. FOCUS 1.0 minimum columns for discovery are defined in code (21 columns)
6.3. Column type mappings use a normalization system that handles `string`/`varchar`, `map<string,string>`/`map<varchar,varchar>`, etc.
6.4. Special column handling (like `billing_period`) is well-documented
6.5. Python code reads columns from YAML config (required parameter, no fallback)

## Technical Requirements

### FOCUS Schema Versions
- **FOCUS 1.0**: Minimum supported version for discovery
- **FOCUS 1.2**: Target schema for the consolidation view
- Must handle tables at any version between 1.0 and 1.2

### FOCUS 1.0 Minimum Columns (for discovery)
Required columns to identify a table as FOCUS-compliant (21 columns, common across all providers):
- `billedcost`
- `billingaccountid`
- `billingcurrency`
- `billingperiodstart`
- `chargecategory`
- `chargedescription`
- `chargeperiodstart`
- `commitmentdiscountcategory`
- `commitmentdiscountid`
- `commitmentdiscountname`
- `effectivecost`
- `listcost`
- `listunitprice`
- `pricingcategory`
- `pricingquantity`
- `pricingunit`
- `servicecategory`
- `servicename`
- `skuid`
- `skupriceid`
- `subaccountid`

### FOCUS 1.2 Target Schema
The consolidation view includes all FOCUS 1.2 columns (58 columns total):
- Core billing columns (BilledCost, EffectiveCost, ListCost, ContractedCost)
- Account identifiers (BillingAccountId, BillingAccountName, BillingAccountType, SubAccountId, SubAccountName, SubAccountType)
- Resource identifiers (ResourceId, ResourceName, ResourceType)
- Service identifiers (ServiceName, ServiceCategory, ServiceSubCategory)
- Commitment discount columns (including CommitmentDiscountQuantity, CommitmentDiscountUnit)
- Capacity reservation columns (CapacityReservationId, CapacityReservationStatus)
- Pricing columns (including PricingCurrency, PricingCurrencyContractedUnitPrice, PricingCurrencyEffectiveCost, PricingCurrencyListUnitPrice)
- Invoice columns (InvoiceId, InvoiceIssuerName)
- SKU columns (SkuId, SkuMeter, SkuPriceId, SkuPriceDetails)
- Metadata columns (Tags, billing_period)

### YAML Column Types
The YAML uses Athena-friendly type names:
- `string` (normalized to `VARCHAR`)
- `double`
- `timestamp`
- `map<string,string>` (normalized to `MAP<VARCHAR,VARCHAR>`)

### Special Column Handling

#### billing_period
- **AWS/Azure**: Exists as a partition column or regular column
- **GCP/OCI**: Must be computed as `date_format(CAST(billingperiodstart AS DATE), '%Y-%m')`
- **Logic**: If column exists in source and is string-compatible, use directly; if it's a date/timestamp, format it; if absent, compute from billingperiodstart

#### Type Casting
- Type compatibility system normalizes types before comparison (e.g., `string` ≡ `varchar`, `float` ≡ `double`)
- All map types are considered compatible with each other
- Array-type source columns mapping to scalar targets produce `CAST(NULL AS <type>)` instead of invalid CAST
- Unknown types default to `VARCHAR`

#### NULL Placeholders
- Use typed NULLs for missing columns via `_resolve_athena_type()`
- Example: `CAST(NULL AS VARCHAR)` for missing string columns
- Example: `CAST(NULL AS DOUBLE)` for missing numeric columns
- Example: `CAST(NULL AS MAP<VARCHAR,VARCHAR>)` for missing map columns

### Tag Discovery Query Fix
The tag discovery query in downstream views (`focus_resource_view`, `focus_summary_view`) is updated:
- UNNEST is moved inside the subquery (before LIMIT) to ensure deterministic tag discovery
- LIMIT raised from 10,000 to 100,000 to cover large datasets
- Original query applied LIMIT to raw rows before UNNEST, causing non-deterministic results

## Dependencies
- Existing `athena.find_tables_with_columns()` method
- Existing `athena.list_databases()` method
- Existing `athena.get_view_diff()` method
- Existing `athena.wait_for_view()` method
- Existing view update logic in `common.py`
- `cid.utils.get_parameter`, `get_yesno_parameter`, `unset_parameter`, `cid_print`, `isatty`
- FOCUS specification documentation

## Out of Scope
- Validation of FOCUS data quality
- Transformation of non-FOCUS tables to FOCUS format
- Support for FOCUS versions below 1.0
- Performance optimization of the consolidated view
- Automatic schema migration for breaking changes

## Success Metrics
- Customers can deploy FOCUS dashboard without manual SQL editing
- `--recursive` updates successfully regenerate `focus_consolidation_view`
- Multi-cloud customers see consolidated data from all providers
- Zero manual intervention required for adding new FOCUS tables
- Tag discovery returns consistent results across runs

## References
- FOCUS specification: https://focus.finops.org/
- Current implementation: `dashboards/focus/focus_consolidation_view/focus_consolidation_view.sql`
- Similar pattern: `cid/helpers/cur_proxy.py` (CUR proxy view generation)
