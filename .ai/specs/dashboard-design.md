# Dashboard Design and Interaction Standards

How to lay out, label and wire the visuals of a dashboard, and how to author the QuickSight definition inside a `cid-cmd` resource file without deploy surprises. Complements `dashboard-standards.md` (resource files, views, datasets). Section 7 is a review checklist.

---

## 1. Keep views flat, put logic in the dashboard

- A view is a plain `SELECT` over the source table: rename, cast, and extract JSON fields with `json_extract_scalar`. Avoid subqueries, array parsing, `UNNEST` or CTE chains unless the data can't be used otherwise.
- Put presentation and business logic in **calculated fields** in the dashboard: classification (category, type, tier), parsing of identifiers (for example splitting an ARN into role and session), rates, outcome labels, display names (`name : id`), and unit switches. They're visible, editable by customers, and don't need a view change and a SPICE schema change to adjust.
- Don't add a derived column to the view when a calculated field can compute it. Removing a view column later forces a dataset update (`--recursive`).
- If a classification is a heuristic (for example derived from the shape of an ID), say so in the visual subtitle or the documentation and prefer a real source field when one exists.
- Use the same terms the dashboard already uses. Pick one word for the counted thing (for example "requests" or "invocations") and use it in every title, subtitle and alt text.

## 2. Tell the story from summary to detail

- Split a large dashboard into tabs, one question per tab (usage, reliability, capacity or cost drivers, raw data), and write the question in the tab description. Keep one 6-month-scale overview first.
- Order each tab top to bottom: KPIs → global controls → trends → breakdowns → ranked tables → **a request-level detail table last**. Every tab ends with a detail table that has enough identifying columns to act on a finding (time, ID, account, region, principal, resource or model, operation, status and error code, plus the measures).
- KPI rows: equal-size tiles, two short rows rather than one tall block. Keep one health KPI (for example error rate) on the summary tab and the details on a health tab.
- Don't show the same breakdown twice (for example a pie and a bar of one dimension). Keep the more informative one.
- A control that drives several visuals (group-by, metric switch) goes directly under the KPIs, to the left, spanning the rows it controls.

## 3. Interactions

- Add a click filter action to every visual **except** KPIs, time trends, heat maps and detail tables. Restrict it to the visual's non-date dimensions (`SelectedFields`), never the date axis, so a click means "this category", not "this day". Actions only filter visuals on the same sheet.
- Dashboard-wide filters: give every filter a `DefaultFilterControlConfiguration` with a human title. Without it the control is titled "<column> equals". A filter that applies to several sheets needs this default and a cross-sheet control.
- A filter on a column with NULLs (for example an error code) must use `NullOption: ALL_VALUES`, or it hides every row that has no value.
- A per-column filter row above a table: fix the column widths, place each control at the same x offset and width, and only filter the columns that fit the canvas width. Controls don't scroll with the table. A text control uses `CustomFilterConfiguration` with `CategoryValue` and **no** `SelectAllOptions`.
- Parameters:
  - **Group By**: a string parameter plus one calculated field `ifelse(${Param} = 'A', {a}, ${Param} = 'B', {b}, … NULL)`. Keep the `// Add ${Param}` marker line so `cid-cmd` can patch options. Show the selected value in titles with `<parameter>${Param}</parameter>`.
  - **Metric switch** (for example tokens or requests): one calculated field `ifelse(${Metric} = 'X', sum({x}), count({id}))` used as the measure everywhere it applies. Put the metric in the visual title the same way, and set an explicit default.

## 4. Visuals

- **Measures**: sum numeric measures (`SUM`), don't `COUNT` the column. Format large numbers with auto scale (K/M/B) and one decimal. Use the percentage format for rates.
- **Outcome mix**: a 100% stacked bar per day with one segment per outcome, so the bars total 100%. Fixed semantic colours across the whole dashboard: success muted green (not a bright green), failures red, throttling orange, other error types distinct and consistent.
- **Donuts**: show category and value as data labels, hide the legend, show the total in the centre, top 10 plus "Other". A trend bar for the same dimension sits under each donut and carries the same title.
- **Stacked bars**: set the colour/segment limit to 10 with "Other" included and the data-point limit to 1000. Check the arithmetic before relying on it: points ≈ days × (segments + 1). Daily data for 6 months is about 2,000 points, hourly data for the same period exceeds the 1,000 category limit.
- **Ranking**: a horizontal bar sorted descending with value labels, top 10 plus "Other". Put the secondary measures in the tooltip (`ColumnTooltipItem`) instead of a second axis.
- **Ranked tables**: add a sparkline (trend over time) and a data bar (the ranking measure) as in-cell visuals (`TableInlineVisualizations`), sort by the ranking measure descending, and use one accent colour.
- **Heat maps and calendars**: colour by the main measure, put the second measure in the tooltip.
- **Account display**: use a calculated field that shows `name : id` and falls back to the id. Names come from `account_map` (see `dashboard-standards.md`), which can be NULL for accounts it doesn't know, so never group or filter on the name column alone.
- **Legends and titles**: put legends at the bottom or hide them when labels already carry the names. Give a legend enough width so series names are readable.

## 5. QuickSight definition pitfalls

- **Calculated fields**: the string lower-case function is `toLower`; `split(expr, delimiter, position)` is 1-based; use `ifelse` with `OR`/`AND`; a calculated field may reference another one; an aggregated calculated field is used as a measure with a `Column` and no `AggregationFunction`.
- **Cloned visuals**: give every clone new `VisualId`, `FieldId`s and `CustomActionId`s. Remove tooltip, drill or sort settings that reference field IDs of the original, or the update fails with "Field ID referred in the tooltip is invalid".
- **Percent format** nests as `FormatConfiguration: { FormatConfiguration: { PercentageDisplayFormatConfiguration: … } }`; number scale is `NumberDisplayFormatConfiguration`.
- **Conflicting settings**: a category filter can define only one of `CategoryValue`, `ParameterName` or `SelectAllOptions`; a filter scoped to several sheets needs `DefaultFilterControlConfiguration`.
- **Edit definitions programmatically**: load the YAML, change the structure, dump it, and verify a no-op round trip first. Keep a view stored as a literal block (`data: |-`) so the SQL stays reviewable in a diff.
- **Dataset changes**: adding or removing a column needs `cid-cmd update --force --recursive --on-drift override` so the view and dataset change together; definition-only changes work with a plain `update --force`. Then refresh SPICE.

## 6. Verify

- Run every new calculated field's logic in Athena and compare the numbers with the visual (totals, top N, rates).
- Check the data with the shapes customers will really have: failures, throttling, NULL names, ARNs of different forms, empty previews, and one long-running range.
- After deploy, confirm the dashboard version reaches `CREATION_SUCCESSFUL` with no errors, then open each tab: titles, parameter text in titles, click filters, filter controls, column widths, number formats and colours.

## 7. Review checklist

- [ ] View is a flat select; logic lives in calculated fields (section 1).
- [ ] Tabs answer one question each; summary → detail; every tab ends with a request-level detail table (section 2).
- [ ] Click filters on the non-date dimension of every chart; filters have titles and keep NULL rows (section 3).
- [ ] Group By and metric parameters use the standard pattern and show their value in the titles (section 3).
- [ ] Measures are summed, auto-scaled; outcome colours are consistent; donuts, stacked bars and tables follow section 4.
- [ ] No duplicate visuals; one wording for the counted thing.
- [ ] Definition edits were verified to deploy (cloned IDs, tooltips, filters) and the right `cid-cmd update` flags were used (section 5).
- [ ] Numbers verified in Athena and each tab opened after deploy (section 6).
