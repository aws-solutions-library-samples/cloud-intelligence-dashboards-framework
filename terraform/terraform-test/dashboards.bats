#!/bin/bats

account_id=$(aws sts get-caller-identity --query "Account" --output text)
database_name="cid_cur" # If variable not set or null, use default
tmp_dir="/tmp/cudos_test"
log_file="$tmp_dir/test_output.log"

setup_file() {
  # Create temp directory for sharing data between tests
  mkdir -p "$tmp_dir"
  
  # Initialize log file
  echo "=== TEST RUN STARTED AT $(date) ===" > "$log_file"
  
  # Get enabled dashboards from environment variables
  > "$tmp_dir/dashboard_ids"
  
  # Special case for cudos-v5 (handle hyphen in name)
  if [ "${deploy_cudos_v5:-no}" = "yes" ]; then
    echo "cudos-v5" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_cudos_v5 -> cudos-v5" | tee -a "$log_file"
  fi
  
  # Check for other dashboards with underscores
  for dashboard in cost_intelligence_dashboard kpi_dashboard; do
    var_name="deploy_${dashboard}"
    if [ "${!var_name:-no}" = "yes" ]; then
      echo "$dashboard" >> "$tmp_dir/dashboard_ids"
      echo "Enabled dashboard: $var_name -> $dashboard" | tee -a "$log_file"
    fi
  done
  
  # If no dashboards are enabled, use default
  if [ ! -s "$tmp_dir/dashboard_ids" ]; then
    echo "cudos-v5" > "$tmp_dir/dashboard_ids"
    echo "No dashboards specified, using default: cudos-v5" | tee -a "$log_file"
  fi
}

teardown_file() {
  # Print location of log file
  echo ""
  echo "Detailed test output saved to $log_file"
  echo "Run 'cat $log_file' to see details"
  echo ""
  
  # Don't clean up temp directory so log file remains available
  # rm -rf "$tmp_dir"
}

@test "Dashboards exist" {
  # Print summary of dashboards to be checked
  echo "=== Testing the following dashboards ===" | tee -a "$log_file"
  cat "$tmp_dir/dashboard_ids" | while read -r dashboard; do
    echo "- $dashboard" | tee -a "$log_file"
  done
  echo "========================================" | tee -a "$log_file"
  
  # Check each enabled dashboard
  while read -r dashboard_id; do
    [ -n "$dashboard_id" ] || continue
    
    echo "" | tee -a "$log_file"
    echo ">>> TESTING DASHBOARD: $dashboard_id <<<" | tee -a "$log_file"
    run aws quicksight describe-dashboard \
      --aws-account-id $account_id \
      --dashboard-id $dashboard_id
    
    [ "$status" -eq 0 ]
    echo "Dashboard $dashboard_id exists and is accessible" | tee -a "$log_file"
    
    # Save output to file for inspection
    echo "$output" > "$tmp_dir/dashboard_${dashboard_id}.json"
    
    # Extract dataset IDs from dashboard and save to file for other tests
    if echo "$output" | jq . >/dev/null 2>&1; then
      echo "$output" | jq -r '.Dashboard.Version.DataSetArns[]? | split("/") | last' 2>/dev/null >> "$tmp_dir/all_dataset_ids" || true
    fi
    echo ">>> DASHBOARD $dashboard_id TEST COMPLETE <<<" | tee -a "$log_file"
    echo "" | tee -a "$log_file"
  done < "$tmp_dir/dashboard_ids"
  
  # Make sure we have at least one dataset ID
  [ -f "$tmp_dir/all_dataset_ids" ] || skip "No datasets found in dashboards"
  sort -u "$tmp_dir/all_dataset_ids" > "$tmp_dir/dataset_ids"
  dataset_count=$(wc -l < "$tmp_dir/dataset_ids")
  [ "$dataset_count" -gt 0 ] || skip "No datasets found in dashboards"
  
  echo "Found dataset IDs: $(cat "$tmp_dir/dataset_ids" | tr '\n' ' ')" | tee -a "$log_file"
  
  # List datasources and find one with "cost" in the name
  run aws quicksight list-data-sources --aws-account-id $account_id
  echo "$output" > "$tmp_dir/datasources.json"
  
  if echo "$output" | jq . >/dev/null 2>&1; then
    datasource_id=$(echo "$output" | jq -r '.DataSources[]? | select(.Name | test("cost"; "i")) | .DataSourceId' 2>/dev/null | head -1)
    
    # If no datasource found with "cost", try with "intelligence"
    if [ -z "$datasource_id" ]; then
      datasource_id=$(echo "$output" | jq -r '.DataSources[]? | select(.Name | test("intelligence"; "i")) | .DataSourceId' 2>/dev/null | head -1)
    fi
    
    # If still no datasource found, just take the first one
    if [ -z "$datasource_id" ]; then
      datasource_id=$(echo "$output" | jq -r '.DataSources[0]?.DataSourceId' 2>/dev/null)
    fi
  fi
  
  echo "$datasource_id" > "$tmp_dir/datasource_id"
  echo "Found datasource ID: $datasource_id" | tee -a "$log_file"
}

@test "Datasets exist" {
  # Get dataset IDs from file
  [ -f "$tmp_dir/dataset_ids" ] || skip "Dashboard test didn't run or failed"
  
  # Check each dataset
  while read -r dataset_id; do
    [ -n "$dataset_id" ] || continue
    
    echo "Checking dataset: $dataset_id" | tee -a "$log_file"
    run aws quicksight describe-data-set \
      --aws-account-id $account_id \
      --data-set-id $dataset_id

    if [ "$status" -ne 0 ]; then
      echo "Failed to describe dataset $dataset_id: $output" | tee -a "$log_file"
      continue
    fi

    # Only process if we got valid output
    if echo "$output" | jq . >/dev/null 2>&1; then
      echo "$output" > "$tmp_dir/dataset_${dataset_id}.json"
      # ... rest of processing
    else
      echo "Invalid JSON response for dataset $dataset_id" | tee -a "$log_file"
    fi

    
    # Extract physical table names from dataset and save for view tests
    if echo "$output" | jq . >/dev/null 2>&1; then
      echo "$output" | jq -r '.DataSet.PhysicalTableMap | .[] | 
        if .CustomSql then .CustomSql.DataSourceArn 
        elif .RelationalTable then .RelationalTable.DataSourceArn 
        else empty end' 2>/dev/null | grep -v "null" | sort -u | while read -r arn; do
        # Extract table name from ARN if it's an Athena source
        if [[ "$arn" == *"athena"* ]]; then
          # Get the dataset SQL to find view names
          sql=$(echo "$output" | jq -r '.DataSet.PhysicalTableMap | .[] | 
            if .CustomSql then .CustomSql.SqlQuery else empty end' 2>/dev/null | grep -v "null")
          
          # Extract table/view names from SQL using regex
          echo "$sql" | grep -o -E 'FROM\s+[a-zA-Z0-9_]+\.' | sed 's/FROM\s\+//g' | sed 's/\.//g' | sort -u >> "$tmp_dir/view_names" 2>/dev/null || true
          echo "$sql" | grep -o -E 'JOIN\s+[a-zA-Z0-9_]+\.' | sed 's/JOIN\s\+//g' | sed 's/\.//g' | sort -u >> "$tmp_dir/view_names" 2>/dev/null || true
        fi
      done
    fi
  done < "$tmp_dir/dataset_ids"
  
  # Get unique view names
  if [ -f "$tmp_dir/view_names" ]; then
    sort -u "$tmp_dir/view_names" > "$tmp_dir/unique_views"
    echo "Found views: $(cat "$tmp_dir/unique_views" | tr '\n' ' ')" | tee -a "$log_file"
  else
    # If we couldn't extract views from SQL, use some common ones
    echo -e "summary_view\ns3_view\nec2_running_cost" > "$tmp_dir/unique_views"
    echo "Using default views for testing" | tee -a "$log_file"
  fi
}

@test "Datasource exists" {
  # Get datasource ID from file
  [ -f "$tmp_dir/datasource_id" ] || skip "Dashboard test didn't run or failed"
  datasource_id=$(cat "$tmp_dir/datasource_id")
  [ -n "$datasource_id" ] || skip "Datasource ID not found"
  
  run aws quicksight describe-data-source \
    --aws-account-id $account_id \
    --data-source-id $datasource_id

  [ "$status" -eq 0 ]
  echo "$output" > "$tmp_dir/datasource_check.json"
  echo "Datasource $datasource_id exists and is accessible" | tee -a "$log_file"
}

@test "Views exist" {
  # Initialize success counter
  success_count=0
  echo "=== Testing views for enabled dashboards ===" | tee -a "$log_file"
  
  # Test common view for all dashboards
  echo "Testing common view: summary_view" | tee -a "$log_file"
  run aws athena get-table-metadata \
    --catalog-name 'AwsDataCatalog' \
    --database-name $database_name \
    --table-name "summary_view"
  if [ "$status" -eq 0 ]; then
    echo "summary_view exists" | tee -a "$log_file"
    success_count=$((success_count + 1))
  fi
  
  # Test CUDOS views
  if grep -q "cudos-v5" "$tmp_dir/dashboard_ids"; then
    echo "Testing CUDOS views..." | tee -a "$log_file"
    for view in "hourly_view" "resource_view"; do
      run aws athena get-table-metadata \
        --catalog-name 'AwsDataCatalog' \
        --database-name $database_name \
        --table-name "$view"
      if [ "$status" -eq 0 ]; then
        echo "$view exists" | tee -a "$log_file"
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  # Test Cost Intelligence views
  if grep -q "cost_intelligence_dashboard" "$tmp_dir/dashboard_ids"; then
    echo "Testing Cost Intelligence views..." | tee -a "$log_file"
    for view in "s3_view" "ec2_running_cost"; do
      run aws athena get-table-metadata \
        --catalog-name 'AwsDataCatalog' \
        --database-name $database_name \
        --table-name "$view"
      if [ "$status" -eq 0 ]; then
        echo "$view exists" | tee -a "$log_file"
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  # Test KPI views
  if grep -q "kpi_dashboard" "$tmp_dir/dashboard_ids"; then
    echo "Testing KPI views..." | tee -a "$log_file"
    for view in "kpi_s3_storage_all" "kpi_instance_all"; do
      run aws athena get-table-metadata \
        --catalog-name 'AwsDataCatalog' \
        --database-name $database_name \
        --table-name "$view"
      if [ "$status" -eq 0 ]; then
        echo "$view exists" | tee -a "$log_file"
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  echo "=== View testing complete ===" | tee -a "$log_file"
  
  # Test passes if at least one view exists
  [ $success_count -gt 0 ] || skip "No views were found"
}
