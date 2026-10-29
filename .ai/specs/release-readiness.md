# Dashboard Release Readiness

Checks to run before releasing a new version of a dashboard: the resource file `dashboards/<name>/<name>.yaml`, its definition `<name>-definition.yaml` (or template), the changelog, and the RSS feed. Run all five checks. Fix what you can, and report the rest with file and line. Section 6 is the checklist.

Dashboard definitions are usually exported from a real QuickSight analysis, which is how customer data, dev names and stale versions get in. Treat every export as untrusted until these checks pass.

---

## 1. No account-specific or hardcoded content

The definition must work in any customer account. Look for values that came from the account the definition was exported from.

| Where | What to look for |
|---|---|
| Pivot table states: `FieldOptions.CollapseStateOptions[].Target.FieldDataPathValues[].FieldValue` (and expanded states) | Real row values: account names, tag values, resource IDs. Keep entries that target a **field** (`FieldId` only). Remove entries that pin a **data value**. |
| Filters: `CategoryValues`, `CategoryValue` | Account IDs, account or customer names, tag values, specific resource IDs or ARNs. Fixed values for **product codes, usage types, operations and charge types** are expected. |
| Controls: `SelectableValues.Values` | Must be generic category values (for example data transfer types), not account or tag values. |
| Parameters: `DefaultValues.StaticValues` | Account IDs, names, tag keys or values, fixed dates that will go stale. |
| Text boxes, titles, subtitles, insight narratives | Customer names, account IDs, email addresses, links to anything that isn't public documentation (internal tools, chat, tickets, personal pages). |
| Anywhere | 12-digit account IDs, `arn:aws:` with a real account, `@<domain>` emails, S3 bucket names, QuickSight user, group or namespace ARNs, hardcoded data source or dataset ARNs (they must be placeholders such as `${athena_datasource_arn}`). |
| Tags | Hardcoded tag **keys** in calculated fields or filters (for example a specific `user_*` cost allocation tag). Tag-based logic must come from parameters, taxonomy fields or `tags_json`, not one customer's keys. |

Documented example IDs (`111122223333`, `123456789012`) are fine.

Quick scan to start from (it prints candidates; review each one). Links are checked against a list of public hosts, so any other host is flagged. If a flagged host is a legitimate public site (for example a product page the dashboard links to), keep the link.

```python
import re, sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
# Public link hosts allowed in a dashboard. Any other host is flagged for review.
PUBLIC_HOSTS = ('aws.amazon.com', 'docs.aws.amazon.com', 'github.com', 'catalog.workshops.aws',
                'catalog.us-east-1.prod.workshops.aws', 'repost.aws',
                'd66mvi2kszsvt.cloudfront.net')  # last one: CID image assets
pat = re.compile(r'(?<![\w-])\d{12}(?![\w-])'                      # account IDs (not UUID parts)
                 r'|arn:aws:[a-z0-9-]+:[a-z0-9-]*:\d{12}'            # ARNs with an account
                 r'|[\w.+-]+@[\w-]+\.[a-z]{2,}')                     # emails
url = re.compile(r'https?://([^/\s"\'<>]+)')
def flagged(text):
    hosts = [h.lower() for h in url.findall(text)]
    return pat.search(text) or any(not h.endswith(PUBLIC_HOSTS) for h in hosts)
def walk(o, path=''):
    if isinstance(o, dict):
        for k, v in o.items(): yield from walk(v, f'{path}/{k}')
    elif isinstance(o, list):
        for i, v in enumerate(o): yield from walk(v, f'{path}[{i}]')
    elif isinstance(o, str):
        if flagged(o) or ('FieldDataPathValues' in path and path.endswith('FieldValue')):
            yield path, o[:120]
for p, v in walk(doc):
    if v not in ('111122223333', '123456789012'):
        print(p, '=>', v)
```

## 2. Spelling and wording

Check every string a user can see:
- sheet names, visual titles and subtitles, text boxes
- control and filter titles, axis and legend `CustomLabel`s, tooltip labels
- calculated-field names, and the string literals that calculated fields return (category labels)

Things to check:
- Typos (for example `Inteligent` for `Intelligent`), double spaces, leading or trailing spaces in labels, and inconsistent casing of the same term.
- AWS service and feature names as written on the AWS pricing or product pages (for example "Amazon OpenSearch Service", "S3 Glacier Instant Retrieval", "Savings Plans").
- The same concept spelled two ways in different visuals or calculated fields.

You can run a spell checker (for example `codespell`) over the extracted strings, but review the results by hand. AWS terms and usage-type names produce false positives.

## 3. Duplicates that break deployment

1. Run the fixer in check mode on the definition:
   ```bash
   python3 scripts/template_fixer/find_duplicate_selected_columns.py dashboards/<name>/<name>-definition.yaml
   ```
   It reports duplicate `SelectedFieldsConfiguration.SelectedColumns` and `SelectedFields` entries. QuickSight rejects these with `Duplicate field in SelectedFieldsConfiguration`.
2. Fix them with `--fix` (add `--force` to skip prompts). The script writes `*.backup` files. **Delete them before you commit.**
3. Also check that these are unique (quick YAML walk):
   - `SheetId`, `VisualId`, `FilterGroupId`, `FilterId`, `ParameterControlId`, `FilterControlId`
   - `ParameterDeclarations` names
   - `CalculatedFields` name per `DataSetIdentifier`. The same name in different datasets is expected and should have the same expression unless the difference is intentional.

## 4. Versions match

| Place | Must contain |
|---|---|
| Resource file `dashboards/<name>/<name>.yaml` → `dashboards.<key>.version` | the new version, for example `v1.2.0` |
| Definition: **the first `vX.Y.Z` in any text box or insight**, usually the About sheet | the same version. `cid-cmd` reads the dashboard version from the first match in text boxes and insights across *all* sheets (`cid/helpers/quicksight/definition.py`). Any other version string that comes first (for example "requires cid-cmd vX.Y.Z") will be read as the dashboard version. |
| Template-based dashboards | the template version referenced by the resource file matches |
| Changelog heading and RSS title (section 5) | the same version |

Also confirm the dev export didn't leak into the resource file: `dashboardId`, `name` and `file` must be the release values (no `dev`, no dates), and `file:` must point to the definition that is in the repo.

## 5. Changelog and RSS

**Changelog:** `changes/CHANGELOG-<name>.md`
- Add a new heading at the top of the version list, in the same format as the others (for example `# <NAME> - 1.2.0`).
- Write one bullet per change: `* **<Sheet>**: <what changed>`. Lead with what the user sees.
- Add a bold **Important:** line when the release requires a minimum `cid-cmd` version, a dataset or view update, or customer action. Include the update command.
- Mention behavior changes that move numbers (for example a category that now shows less cost because part of it was reclassified).

**RSS:** `changes/cloud-intelligence-dashboards.rss`
- Add a new `<item>` as the **first** item in `<channel>`, following the existing dashboard items:
  - `<title>[Update] <Dashboard> Dashboard vX.Y.Z</title>`
  - `<link>`: the changelog anchor on GitHub. The anchor is the heading in lowercase with dots removed, for example `#<name>---120` for `# <NAME> - 1.2.0`. Use the current repository URL.
  - `<pubDate>`: RFC 822 date (`Fri, 23 Oct 2026 12:00:00 GMT`).
  - `<category><![CDATA[Dashboard Update]]></category>`
  - `<guid isPermaLink="false">`: a new UUID (`python3 -c "import uuid; print(uuid.uuid4())"`), never a copy of an earlier one.
  - `<description>`, and `<content:encoded>` with a `<ul>` that matches the changelog bullets.
- Update `<lastBuildDate>` in the channel to the new item's date.
- Check that the feed is still valid XML:
  ```bash
  python3 -c "import xml.etree.ElementTree as E; E.parse('changes/cloud-intelligence-dashboards.rss'); print('ok')"
  ```

## 6. Checklist

- [ ] No customer values in pivot collapse or expand states, filters, controls, parameters or text (section 1 scan reviewed).
- [ ] No account IDs, real ARNs, emails, internal links or customer tag keys. Data sources use placeholders.
- [ ] User-visible strings spell-checked. AWS names are correct. No double, leading or trailing spaces in labels.
- [ ] `find_duplicate_selected_columns.py` reports nothing. IDs, parameter names and calculated fields are unique. No `*.backup` files staged.
- [ ] Version is identical in the resource file, the first `vX.Y.Z` in the definition text, the changelog heading and the RSS title.
- [ ] The resource file has release `dashboardId`, `name` and `file` (no dev values).
- [ ] Changelog entry added at the top, with **Important:** notes where needed.
- [ ] RSS item added first, with a new GUID, correct anchor link and date. `lastBuildDate` updated. XML parses.
