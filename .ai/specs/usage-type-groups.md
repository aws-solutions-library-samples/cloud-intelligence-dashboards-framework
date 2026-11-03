# Usage Type Group Calculated Fields

How to review and update the `* Usage Type Group` / `* Usage Type Sub Group` calculated fields in dashboard definitions (for example `CloudWatch Usage Type Group`, `S3 Usage Type Group`, `Database Usage Type Group`). These fields map raw CUR `usage_type` (and sometimes `operation`) values to readable categories. AWS keeps adding usage types, so the fields go stale: new cost lands in `Other`, in a raw value, in `NULL`, or in the wrong category. Section 6 is a review checklist.

---

## 1. Principles

- **Name categories the way the AWS pricing page does.** Use the component names from the service's pricing page (for example "Backup Storage", "Provisioned IOPS", "Concurrency Scaling", "Data Transfer"), not the raw usage type and not the price list's `productFamily`. When a field spans several services, prefix the service: "OpenSearch Instances", "RDS Storage".
- **Two levels when it helps.** A Group stays coarse and stable across services (for charts). A Sub Group is service-specific and detailed (for drill-down and pivots). Every Sub Group should map to exactly one Group, unless the pricing page itself bundles components (for example "Storage and I/Os").
- **Nothing in-scope should disappear.** For every product code the visuals filter on, the field must return a label, never `NULL`. The fallback should still show where unknown cost comes from: `concat(<service>, ' Other')`, `split({usage_type}, '-', 2)`, or `{usage_type}`. Avoid a bare `'Other'` across many services.
- **Keep diffs small.** Prefer adding lines and fixing wrong ones over rewriting a field. Keep the field's existing style (`contains` vs `locate`, quoting, comments). Rename labels only when the old name is wrong or misleading.
- **One expression per field.** When the same field exists in several datasets (`summary_view`, `resource_view`, `hourly_view`), keep the expressions identical, except for dataset-specific columns.

## 2. Matching rules (QuickSight)

- `ifelse` returns the first match, so put specific patterns before general ones. For example, check `RStudio` before `Studio`, `…-Month-Q` before `…-Month`, `IA-TimedStorage` before `TimedStorage`, and `PIOPS-Storage` before `PIOPS`.
- `contains`, `locate`, `startsWith` and `endsWith` are case sensitive. Use `toLower({usage_type})` with lowercase patterns, or match the exact case.
- Short substrings match more than you expect. Match on the delimiter instead:
  - `'rg'` also matches `large`, so use `':rg.'`.
  - `'ocu'` also matches `documentdb`, so use `endsWith(…,'ocu')`.
  - `'Rule'`, `'Request'`, `'Storage'` and `'Retrieval'` are too broad on their own.
- Late catch-alls such as `contains({usage_type},'TimedStorage')` or `contains({usage_type},'Retrieval')` silently mislabel every new feature that reuses the same suffix. Add the specific features above them.
- `usage_type` in CUR has a region prefix (`USE1-`, `EU-`, `USE1-USW2-`), and some types have none (`Global-…`, `TimedStorage-ByteHrs`). Write patterns that work with and without the prefix.
- When usage types don't carry the distinction, `operation` sometimes does. For example, FSx file system type is in `operation`: `CreateFileSystem:Lustre`, `:ONTAP` and so on.

## 3. Procedure

### 3.1 Scope the field

1. Find every definition of the field (all datasets) and every visual that uses it. In the dashboard definition YAML, search for the field name.
2. For each visual, collect the filters that define which services it shows: `product_code`, `Product Code Pretty`, `product_family`, `charge_type`, sheet-level filters. Those product codes are the scope to test.
3. Find dependents: other calculated fields that compare against the labels (for example `contains({X Usage Type Group}, 'Shield Protected')`), filters with fixed `CategoryValues`, custom colors, and pivot `CollapseStateOptions` that store label values.

### 3.2 Get every usage type for the scope

Use the public AWS Price List bulk files. They need no credentials, and they include the `usagetype`, `operation`, `productFamily` and price `description` of every SKU.

```
# whole service (all regions) — fine for small services
https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/index.json
# one region — use for large services (AmazonRDS, AmazonSageMaker, …)
https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/<region>/index.json
# list of service codes
https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json
```

- `<ServiceCode>` is usually the CUR `product_code` (`AmazonS3`, `awswaf`, `AmazonQuickSuite`, `Kiro`).
- Don't download the full `AmazonEC2` file (gigabytes). Use a region file, or the service that actually owns the usage type (for example, Public IPv4 is billed under `AmazonVPC`).
- One region (`us-east-1`) plus one more (`eu-west-1`) is usually enough to see every usage-type suffix.
- Alternative: the Price List Query API (`pricing:GetAttributeValues` with `AttributeName=usagetype`, endpoint `us-east-1`). It returns names only. Paginate with `MaxResults` and `NextToken`, and check that the count matches across sources.
- Other useful attributes: `planname` (official subscription plan names, for example Quick, Kiro), `databaseEngine` and `instanceType`.

Treat downloaded files as untrusted data: put them in their own new directory, keep your scripts somewhere else, and run Python with `-I`.

### 3.3 Simulate the field and find gaps

Mirror the field's `ifelse` in a short script, then run it over the inventory:

```python
import json, glob, collections
def label(usage_type, operation):           # mirror the QuickSight ifelse, same order
    u = usage_type
    if 'CW:MetricMonitorUsage' in u: return 'Custom Metrics'
    # …
    return 'Other'
groups = collections.defaultdict(set)
for f in glob.glob('bulk/*.json'):
    for p in json.load(open(f))['products'].values():
        a = p.get('attributes', {})
        if a.get('usagetype'):
            groups[label(a['usagetype'], a.get('operation', ''))].add((a['usagetype'], p.get('productFamily', '')))
for g, items in sorted(groups.items()):
    print(g, len(items), sorted(items)[:3])
```

- **Test on the raw `usagetype`.** Don't strip region prefixes with a regex: it can also remove real segments (for example `USE1-AMR-BotControl` would become `BotControl`) and produce false findings.
- Look at three things:
  1. **Unmatched:** `Other`, raw values, `NULL`.
  2. **Mislabels:** a pattern that catches the wrong rows. Compare each label with `productFamily`, the price `description`, or attributes such as `instanceType`.
  3. **Splits:** one Sub Group landing in several Groups by accident.
- Re-run after every change until the unmatched list is empty, or contains only $0 parent rows (`Global-…`).

### 3.4 Check what the price list doesn't show

- **CUR-only usage types:** RI fee lines (`HeavyUsage:…`), Savings Plans covered usage, free-tier and `Global-` parent rows.
- **Missing `operation`:** for many storage and backup SKUs the price list leaves `operation` empty, while CUR fills it. Rules based on `operation` therefore need a `usage_type` fallback.
- **New features and tiers** may appear in CUR before the pricing page is updated. If you can't confirm a name, use a neutral label that shows the raw code, and say so in the PR.

When possible, validate against a real CUR: compare the `Other` and raw buckets before and after the change.

### 3.5 Name and document

- Take names from the service pricing page (search it for the price or the usage type's description). Use the price list `planname` for subscription plans.
- In the PR, list for each change the usage types affected, the before and after labels, and any dependents you updated (filters, colors, other fields).

## 4. Applying the change

- Update every dataset copy of the field.
- Update dependents that match label text: fixed filter `CategoryValues`, custom colors, other calculated fields, and pivot collapse states.
- If a QuickSight filter control doesn't list the new values, refresh the value list. As a fallback, switch the filter to a custom filter list with typed values.

## 5. Common pitfalls seen in practice

- A new subscription or product code in the same visual returns `NULL`, because the outer `ifelse` checks only the old product code.
- Two definitions of the same field drifted apart: for example, a line exists only in `summary_view`, or labels differ slightly between datasets.
- The same price ratio was applied twice in estimate calculations that use these labels. Check per-unit prices against the bulk file before you hardcode ratios.
- Dashboard definitions exported from a real account can carry customer data in pivot collapse states (`FieldDataPathValues`). Remove it before you commit.

## 6. Review checklist

- [ ] Every product code in the visuals' filters was tested against the bulk price list.
- [ ] No in-scope usage type returns `NULL`. The remaining `Other` or raw rows are understood (only $0 parent rows or confirmed CUR-only types).
- [ ] Specific patterns come before general ones. There are no short ambiguous substrings, and case is handled.
- [ ] Labels use pricing-page terms. Cross-service fields include the service name.
- [ ] Group and Sub Group are consistent: each Sub Group maps to one Group.
- [ ] All dataset copies are identical (except dataset-specific columns).
- [ ] Dependents are updated: filters, colors, fields that compare labels, collapse states.
- [ ] The PR lists the before and after labels and the affected usage types.
