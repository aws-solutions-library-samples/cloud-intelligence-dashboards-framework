#!/usr/bin/env python3
"""
Find duplicate filters in a QuickSight dashboard definition YAML.

Detects cases where a filter applied to ALL_VISUALS on a sheet is also
applied (with equivalent logic) to SELECTED_VISUALS on the same sheet,
or where multiple SELECTED_VISUALS filter groups on the same sheet have
equivalent filter definitions with overlapping visual IDs.

A filter is considered a "duplicate" when:
  1. Two filter groups target the same sheet
  2. One has Scope=ALL_VISUALS, the other has Scope=SELECTED_VISUALS
     (or both target overlapping visuals)
  3. They filter on the same column with equivalent configuration
"""

import re
import sys
import json
import copy
import yaml
from collections import defaultdict
from pathlib import Path


def normalize_filter(f):
    """Return a hashable signature of a filter's logical meaning (ignoring FilterId)."""
    f = copy.deepcopy(f)
    filter_type = list(f.keys())[0]
    body = f[filter_type]
    body.pop('FilterId', None)
    return json.dumps({filter_type: body}, sort_keys=True)


def extract_scope_info(fg):
    """Extract sheet-level scope info from a filter group."""
    scopes = []
    scope_cfg = fg.get('ScopeConfiguration', {})
    selected = scope_cfg.get('SelectedSheets', {})
    for svc in selected.get('SheetVisualScopingConfigurations', []):
        scopes.append({
            'scope': svc.get('Scope'),
            'sheet_id': svc.get('SheetId'),
            'visual_ids': set(svc.get('VisualIds', [])),
        })
    return scopes


def build_sheet_name_map(sheets):
    """Build SheetId → sheet name mapping."""
    mapping = {}
    for sheet in sheets:
        sid = sheet.get('SheetId', '')
        name = sheet.get('Title') or sheet.get('Name') or sid
        mapping[sid] = name
    return mapping


def _extract_visual_title(visual_body):
    """Extract a human-readable title from a visual's body."""
    title_obj = visual_body.get('Title', {})
    fmt = title_obj.get('FormatText', {})
    if 'PlainText' in fmt:
        return fmt['PlainText'].strip()
    if 'RichText' in fmt:
        # Strip XML tags to get plain text
        raw = fmt['RichText']
        text = re.sub(r'<[^>]+>', '', raw).strip()
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text)
        return text if text else None
    return None


# All known QuickSight visual type keys
VISUAL_TYPES = [
    'BarChartVisual', 'BoxPlotVisual', 'ComboChartVisual', 'CustomContentVisual',
    'DonutChartVisual', 'EmptyVisual', 'FilledMapVisual', 'FunnelChartVisual',
    'GaugeChartVisual', 'GeospatialMapVisual', 'HeatMapVisual', 'HistogramVisual',
    'InsightVisual', 'KPIVisual', 'LineChartVisual', 'PieChartVisual',
    'PivotTableVisual', 'RadarChartVisual', 'SankeyDiagramVisual',
    'ScatterPlotVisual', 'TableVisual', 'TreeMapVisual', 'WaterfallVisual',
    'WordCloudVisual',
]


def build_visual_name_map(sheets):
    """Build VisualId → (visual_type, title) mapping by walking all sheets."""
    mapping = {}
    for sheet in sheets:
        for visual_wrapper in sheet.get('Visuals', []):
            for vtype in VISUAL_TYPES:
                if vtype in visual_wrapper:
                    body = visual_wrapper[vtype]
                    vid = body.get('VisualId', '')
                    title = _extract_visual_title(body) or '(untitled)'
                    short_type = vtype.replace('Visual', '')
                    mapping[vid] = f"{title} [{short_type}]"
                    break
    return mapping


def resolve_visual(vid, visual_names):
    """Return 'Title [Type]' or fall back to the raw ID."""
    return visual_names.get(vid, vid)


def resolve_sheet(sid, sheet_names):
    """Return sheet name or fall back to the raw ID."""
    return sheet_names.get(sid, sid)


def _describe_filters(filters):
    """Return a short human-readable description of the filter(s)."""
    parts = []
    for f in filters:
        ftype = list(f.keys())[0]
        body = f[ftype]
        col = body.get('Column', {})
        col_name = col.get('ColumnName', '?')
        ds = col.get('DataSetIdentifier', '?')
        parts.append(f"{ftype} on {ds}.{col_name}")
    return '; '.join(parts) if parts else '(unknown)'


def main():
    if len(sys.argv) < 2:
        print("Usage: python find_duplicate_filters.py <definition.yaml>")
        sys.exit(1)

    path = Path(sys.argv[1])
    print(f"Loading {path} ...")
    with open(path) as fh:
        definition = yaml.safe_load(fh)

    sheets = definition.get('Sheets', [])
    sheet_names = build_sheet_name_map(sheets)
    visual_names = build_visual_name_map(sheets)

    filter_groups = definition.get('FilterGroups', [])
    print(f"Found {len(filter_groups)} filter groups, {len(sheets)} sheets, {len(visual_names)} visuals.\n")

    # ── Build index: (sheet_id, filter_signature) → list of filter groups ──
    Index = defaultdict(list)

    for fg in filter_groups:
        fg_id = fg.get('FilterGroupId', '?')
        status = fg.get('Status', 'ENABLED')
        filters = fg.get('Filters', [])
        scopes = extract_scope_info(fg)
        filter_sigs = tuple(sorted(normalize_filter(f) for f in filters))

        for sc in scopes:
            key = (sc['sheet_id'], filter_sigs)
            Index[key].append({
                'filter_group_id': fg_id,
                'scope': sc['scope'],
                'visual_ids': sc['visual_ids'],
                'status': status,
                'filters': filters,
            })

    # ── Find duplicates ──
    duplicates_found = 0

    for (sheet_id, _), entries in sorted(Index.items()):
        if len(entries) < 2:
            continue

        all_vis = [e for e in entries if e['scope'] == 'ALL_VISUALS']
        selected = [e for e in entries if e['scope'] == 'SELECTED_VISUALS']

        # Case 1: ALL_VISUALS + SELECTED_VISUALS on same sheet with same filter
        if all_vis and selected:
            for av in all_vis:
                for sv in selected:
                    duplicates_found += 1
                    _print_dup_header(duplicates_found,
                        "ALL_VISUALS filter also applied to specific visuals",
                        sheet_id, sheet_names, av['filters'])
                    print(f"  ▸ ALL_VISUALS filter group: {av['filter_group_id']}  (status={av['status']})")
                    print(f"  ▸ SELECTED_VISUALS filter group: {sv['filter_group_id']}  (status={sv['status']})")
                    _print_visuals("    Visuals", sv['visual_ids'], visual_names)
                    print()

        # Case 2: Multiple SELECTED_VISUALS with overlapping visual IDs
        if len(selected) >= 2:
            for i in range(len(selected)):
                for j in range(i + 1, len(selected)):
                    a, b = selected[i], selected[j]
                    overlap = a['visual_ids'] & b['visual_ids']
                    if overlap:
                        duplicates_found += 1
                        _print_dup_header(duplicates_found,
                            "Same filter applied to overlapping visuals",
                            sheet_id, sheet_names, a['filters'])
                        print(f"  ▸ Filter group A: {a['filter_group_id']}  (status={a['status']})")
                        _print_visuals("    Visuals", a['visual_ids'], visual_names)
                        print(f"  ▸ Filter group B: {b['filter_group_id']}  (status={b['status']})")
                        _print_visuals("    Visuals", b['visual_ids'], visual_names)
                        _print_visuals("    Overlapping", overlap, visual_names)
                        print()

    print("=" * 90)
    print(f"\nTotal duplicates found: {duplicates_found}")
    if duplicates_found == 0:
        print("No duplicate filters detected. ✓")


def _print_dup_header(num, desc, sheet_id, sheet_names, filters):
    print("=" * 90)
    print(f"DUPLICATE #{num}: {desc}")
    print(f"  Sheet: {resolve_sheet(sheet_id, sheet_names)}")
    print(f"  Filter: {_describe_filters(filters)}")
    print()


def _print_visuals(label, visual_ids, visual_names):
    resolved = [resolve_visual(v, visual_names) for v in sorted(visual_ids)]
    for v in resolved:
        print(f"{label}: {v}")


if __name__ == '__main__':
    main()
