#!/usr/bin/env python3
import sys
import os
from pathlib import Path

def find_duplicates(lines):
    changes = []
    i = 0
    
    while i < len(lines):
        if 'SelectedFieldsConfiguration:' in lines[i]:
            j = i + 1
            while j < len(lines) and 'SelectedColumns:' not in lines[j]:
                j += 1
            
            if j >= len(lines):
                i += 1
                continue
            
            col_entries = []
            k = j + 1
            has_target_visuals = False
            
            while k < len(lines):
                line = lines[k]
                if line.strip().startswith('- ColumnName:'):
                    col_name = line.split('ColumnName:')[1].strip()
                    dataset = None
                    if k + 1 < len(lines) and 'DataSetIdentifier:' in lines[k + 1]:
                        dataset = lines[k + 1].split('DataSetIdentifier:')[1].strip()
                    col_entries.append((k, k + 2, col_name, dataset))
                    k += 2
                elif line.strip().startswith('SelectedFields:'):
                    break
                elif line.strip().startswith('TargetVisualsConfiguration:'):
                    has_target_visuals = True
                    break
                elif line.strip() and not line.strip().startswith('-') and ':' in line:
                    break
                else:
                    k += 1
            
            field_entries = []
            if k < len(lines) and 'SelectedFields:' in lines[k]:
                k += 1
                while k < len(lines):
                    line = lines[k]
                    if line.strip().startswith('- ') and not 'ColumnName:' in line:
                        field_val = line.strip()[2:]
                        field_entries.append((k, k + 1, field_val))
                        k += 1
                    elif line.strip() and not line.strip().startswith('-') and ':' in line:
                        break
                    else:
                        k += 1
            
            # Only process SelectedColumns duplicates if no TargetVisuals
            if not has_target_visuals and len(col_entries) > 1:
                idx = 0
                while idx < len(col_entries):
                    curr_col = (col_entries[idx][2], col_entries[idx][3])
                    dup_indices = [idx]
                    
                    j = idx + 1
                    while j < len(col_entries):
                        next_col = (col_entries[j][2], col_entries[j][3])
                        if next_col == curr_col:
                            dup_indices.append(j)
                            j += 1
                        else:
                            break
                    
                    if len(dup_indices) > 1:
                        to_remove_cols = [(col_entries[i][0], col_entries[i][1]) for i in dup_indices[1:]]
                        to_remove_fields = []
                        
                        # Remove same number of SelectedFields as SelectedColumns
                        if len(field_entries) >= len(dup_indices):
                            to_remove_fields = [(field_entries[i][0], field_entries[i][1]) for i in dup_indices[1:]]
                        
                        changes.append((col_entries[dup_indices[0]][0], to_remove_cols, to_remove_fields, curr_col))
                    
                    idx = j if j > idx else idx + 1
            
            # Handle SelectedFields duplicates when only 1 SelectedColumn
            elif len(col_entries) == 1 and len(field_entries) > 1:
                idx = 0
                while idx < len(field_entries):
                    curr_field = field_entries[idx][2]
                    dup_indices = [idx]
                    
                    j = idx + 1
                    while j < len(field_entries) and field_entries[j][2] == curr_field:
                        dup_indices.append(j)
                        j += 1
                    
                    if len(dup_indices) > 1:
                        to_remove_fields = [(field_entries[i][0], field_entries[i][1]) for i in dup_indices[1:]]
                        changes.append((col_entries[0][0], [], to_remove_fields, (col_entries[0][2], col_entries[0][3])))
                    
                    idx = j if j > idx else idx + 1
            
            i = k
        else:
            i += 1
    
    return changes

def show_diff(lines, keep_line, remove_cols, remove_fields):
    print("\nWill keep:")
    print(f"  Line {keep_line}: {lines[keep_line].rstrip()}")
    if keep_line + 1 < len(lines):
        print(f"  Line {keep_line + 1}: {lines[keep_line + 1].rstrip()}")
    
    if remove_cols:
        print(f"\nWill remove from SelectedColumns ({len(remove_cols)} items):")
        for start, end in remove_cols:
            for line_num in range(start, end):
                print(f"  Line {line_num}: {lines[line_num].rstrip()}")
    
    if remove_fields:
        print(f"\nWill remove from SelectedFields ({len(remove_fields)} items):")
        for start, end in remove_fields:
            for line_num in range(start, end):
                print(f"  Line {line_num}: {lines[line_num].rstrip()}")

def process_file(yaml_file, fix_mode, force_mode):
    """Process a single YAML file"""
    if fix_mode:
        print(f"\n{'='*60}")
        print(f"Processing: {yaml_file}")
        print(f"{'='*60}")
        
        backup_file = yaml_file + '.backup'
        with open(yaml_file, 'r') as f:
            with open(backup_file, 'w') as backup:
                content = f.read()
                backup.write(content)
        print(f"Backup saved to: {backup_file}\n")
        
        fixed_count = 0
        while True:
            with open(yaml_file, 'r') as f:
                lines = f.readlines()
            
            changes = find_duplicates(lines)
            
            if not changes:
                if fixed_count > 0:
                    print(f"\nAll duplicates fixed! Total fixes applied: {fixed_count}")
                break
            
            keep_line, remove_cols, remove_fields, (col_name, dataset) = changes[0]
            
            print(f"\n{'='*60}")
            print(f"Found duplicate: '{col_name}' (DataSet: '{dataset}')")
            if remove_cols:
                print(f"Repeated {len(remove_cols) + 1} times")
            show_diff(lines, keep_line, remove_cols, remove_fields)
            
            if not force_mode:
                response = input("\nApply this fix? (y/n/q): ").strip().lower()
                
                if response == 'q':
                    print("Quitting...")
                    return False
                elif response != 'y':
                    continue
            
            lines_to_delete = set()
            for start, end in remove_cols:
                for line_num in range(start, end):
                    lines_to_delete.add(line_num)
            for start, end in remove_fields:
                for line_num in range(start, end):
                    lines_to_delete.add(line_num)
            
            new_lines = [line for i, line in enumerate(lines) if i not in lines_to_delete]
            
            with open(yaml_file, 'w') as f:
                f.writelines(new_lines)
            
            fixed_count += 1
            print(f"✓ Fixed! ({fixed_count} total)")
        return True
    else:
        with open(yaml_file, 'r') as f:
            lines = f.readlines()
        
        changes = find_duplicates(lines)
        
        if changes:
            print(f"\n{'='*60}")
            print(f"File: {yaml_file}")
            print(f"Found {len(changes)} locations with consecutive duplicates\n")
            for keep_line, remove_cols, remove_fields, (col_name, dataset) in changes:
                print(f"\nDuplicate: '{col_name}' (DataSet: '{dataset}')")
                if remove_cols:
                    print(f"Repeated {len(remove_cols) + 1} times")
                show_diff(lines, keep_line, remove_cols, remove_fields)
            return True
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 find_duplicate_selected_columns.py <yaml_file_or_folder> [--fix] [--force]")
        print("  <yaml_file_or_folder>: Path to a YAML file or folder containing YAML files")
        print("  --fix: Apply fixes (prompts for confirmation unless --force is used)")
        print("  --force: Apply all fixes without confirmation (requires --fix)")
        sys.exit(1)
    
    path = sys.argv[1]
    fix_mode = '--fix' in sys.argv
    force_mode = '--force' in sys.argv
    
    if force_mode and not fix_mode:
        print("Error: --force requires --fix")
        sys.exit(1)
    
    # Check if path is a file or directory
    if os.path.isfile(path):
        yaml_files = [path]
    elif os.path.isdir(path):
        yaml_files = [str(f) for f in Path(path).rglob('*.yaml')] + [str(f) for f in Path(path).rglob('*.yml')]
        if not yaml_files:
            print(f"No YAML files found in {path}")
            sys.exit(1)
        print(f"Found {len(yaml_files)} YAML files in {path}")
    else:
        print(f"Error: {path} is not a valid file or directory")
        sys.exit(1)
    
    if fix_mode:
        print(f"Running in FIX mode{' (no confirmation)' if force_mode else ' - will prompt for each change'}\n")
    else:
        print("Running in DRY RUN mode\n")
    
    files_with_issues = 0
    for yaml_file in yaml_files:
        try:
            has_issues = process_file(yaml_file, fix_mode, force_mode)
            if has_issues:
                files_with_issues += 1
            if not fix_mode and not has_issues:
                continue  # Skip files with no issues in dry run
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\nError processing {yaml_file}: {e}")
            continue
    
    if not fix_mode:
        if files_with_issues == 0:
            print("\nNo consecutive duplicate SelectedColumns entries found.")
        else:
            print(f"\n{'='*60}")
            print(f"\nFound issues in {files_with_issues} file(s)")
            print("Run with --fix to apply changes")
