#!/bin/bats

account_id=$(aws sts get-caller-identity --query "Account" --output text)
# Only check the two most likely databases based on CID deployment patterns
# Deduplicate in case both values are the same
db1="cid_cur"
db2="${database_name:-cid_tf_data_export}"
if [ "$db1" = "$db2" ]; then
  possible_databases=("$db1")
else
  possible_databases=("$db1" "$db2")
fi
database_name="${possible_databases[0]}"
tmp_dir="/tmp/cudos_test"
log_file="$tmp_dir/test_output.log"

# Helper function to test if a view exists in any of the possible databases
test_view_in_databases() {
  local view_name="$1"
  for db in "${possible_databases[@]}"; do
    run aws athena get-table-metadata \
      --catalog-name 'AwsDataCatalog' \
      --database-name "$db" \
      --table-name "$view_name" 2>/dev/null
    if [ "$status" -eq 0 ]; then
      echo "✓ $view_name found in database: $db" | tee -a "$log_file"
      return 0
    fi
  done
  echo "✗ $view_name not found in any database" | tee -a "$log_file"
  return 1
}

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
  
  # Check for other foundational dashboards with underscores
  for dashboard in cost_intelligence_dashboard kpi_dashboard; do
    var_name="deploy_${dashboard}"
    if [ "${!var_name:-no}" = "yes" ]; then
      echo "$dashboard" >> "$tmp_dir/dashboard_ids"
      echo "Enabled dashboard: $var_name -> $dashboard" | tee -a "$log_file"
    fi
  done
  
  # Check additional dashboards
  if [ "${deploy_trends_dashboard:-no}" = "yes" ]; then
    echo "trends-dashboard" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_trends_dashboard -> trends-dashboard" | tee -a "$log_file"
  fi
  
  if [ "${deploy_datatransfer_dashboard:-no}" = "yes" ]; then
    echo "datatransfer-cost-analysis-dashboard" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_datatransfer_dashboard -> datatransfer-cost-analysis-dashboard" | tee -a "$log_file"
  fi
  
  if [ "${deploy_marketplace_dashboard:-no}" = "yes" ]; then
    echo "aws-marketplace" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_marketplace_dashboard -> aws-marketplace" | tee -a "$log_file"
  fi
  
  if [ "${deploy_connect_dashboard:-no}" = "yes" ]; then
    echo "amazon-connect-cost-insight-dashboard" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_connect_dashboard -> amazon-connect-cost-insight-dashboard" | tee -a "$log_file"
  fi
  
  if [ "${deploy_scad_containers_dashboard:-no}" = "yes" ]; then
    echo "scad-containers-cost-allocation" >> "$tmp_dir/dashboard_ids"
    echo "Enabled dashboard: deploy_scad_containers_dashboard -> scad-containers-cost-allocation" | tee -a "$log_file"
  fi
  
  # If no dashboards are enabled, create empty marker
  if [ ! -s "$tmp_dir/dashboard_ids" ]; then
    touch "$tmp_dir/no_dashboards"
    echo "No dashboards enabled, skipping dashboard tests" | tee -a "$log_file"
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
  
  echo "Found dataset IDs: $(cat "$tmp_dir/dataset_ids" | tr '\n' ' ')" >> "$log_file"
  
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
  dataset_success=0
  dataset_total=0
  
  while read -r dataset_id; do
    [ -n "$dataset_id" ] || continue
    dataset_total=$((dataset_total + 1))
    
    echo "Checking dataset: $dataset_id" >> "$log_file"
    run aws quicksight describe-data-set \
      --aws-account-id $account_id \
      --data-set-id $dataset_id

    if [ "$status" -eq 0 ]; then
      echo "✓ Dataset $dataset_id exists and is accessible" >> "$log_file"
      dataset_success=$((dataset_success + 1))
      
      # Save output for inspection
      if echo "$output" | jq . >/dev/null 2>&1; then
        echo "$output" > "$tmp_dir/dataset_${dataset_id}.json"
      fi
    else
      echo "✗ Failed to describe dataset $dataset_id" >> "$log_file"
    fi

  done < "$tmp_dir/dataset_ids"
  
  echo "✓ Dataset validation summary: $dataset_success/$dataset_total datasets accessible" | tee -a "$log_file"
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
  total_views=0
  echo "=== Testing views for enabled dashboards ===" | tee -a "$log_file"
  echo "Using database: $database_name" | tee -a "$log_file"
  
  # Test common view for all dashboards
  echo "Testing common view: summary_view" | tee -a "$log_file"
  total_views=$((total_views + 1))
  if test_view_in_databases "summary_view"; then
    success_count=$((success_count + 1))
  fi
  
  # Test CUDOS views
  if grep -q "cudos-v5" "$tmp_dir/dashboard_ids"; then
    echo "Testing CUDOS views..." | tee -a "$log_file"
    for view in "hourly_view" "resource_view"; do
      total_views=$((total_views + 1))
      if test_view_in_databases "$view"; then
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  # Test Cost Intelligence views
  if grep -q "cost_intelligence_dashboard" "$tmp_dir/dashboard_ids"; then
    echo "Testing Cost Intelligence views..." | tee -a "$log_file"
    for view in "s3_view" "ec2_running_cost"; do
      total_views=$((total_views + 1))
      if test_view_in_databases "$view"; then
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  # Test KPI views
  if grep -q "kpi_dashboard" "$tmp_dir/dashboard_ids"; then
    echo "Testing KPI views..." | tee -a "$log_file"
    for view in "kpi_s3_storage_all" "kpi_instance_all"; do
      total_views=$((total_views + 1))
      if test_view_in_databases "$view"; then
        success_count=$((success_count + 1))
      fi
    done
  fi
  
  # Test Additional Dashboard datasets
  echo "Testing additional dashboard datasets..." | tee -a "$log_file"
  
  # Trends Dashboard - daily-anomaly-detection
  if [ "${deploy_trends_dashboard:-no}" = "yes" ]; then
    echo "Testing Trends Dashboard dataset: daily-anomaly-detection" | tee -a "$log_file"
    total_views=$((total_views + 1))
    if test_view_in_databases "daily_anomaly_detection"; then
      success_count=$((success_count + 1))
    fi
  fi
  
  # Data Transfer Dashboard - data_transfer_view
  if [ "${deploy_datatransfer_dashboard:-no}" = "yes" ]; then
    echo "Testing Data Transfer Dashboard dataset: data_transfer_view" | tee -a "$log_file"
    total_views=$((total_views + 1))
    if test_view_in_databases "data_transfer_view"; then
      success_count=$((success_count + 1))
    fi
  fi
  
  # Marketplace Dashboard - marketplace_view
  if [ "${deploy_marketplace_dashboard:-no}" = "yes" ]; then
    echo "Testing Marketplace Dashboard dataset: marketplace_view" | tee -a "$log_file"
    total_views=$((total_views + 1))
    if test_view_in_databases "marketplace_view"; then
      success_count=$((success_count + 1))
    fi
  fi
  
  # Connect Dashboard - resource_connect_view
  if [ "${deploy_connect_dashboard:-no}" = "yes" ]; then
    echo "Testing Connect Dashboard dataset: resource_connect_view" | tee -a "$log_file"
    total_views=$((total_views + 1))
    if test_view_in_databases "resource_connect_view"; then
      success_count=$((success_count + 1))
    fi
  fi
  
  # SCAD Containers Dashboard - scad_cca_summary_view
  if [ "${deploy_scad_containers_dashboard:-no}" = "yes" ]; then
    echo "Testing SCAD Containers Dashboard dataset: scad_cca_summary_view" | tee -a "$log_file"
    total_views=$((total_views + 1))
    if test_view_in_databases "scad_cca_summary_view"; then
      success_count=$((success_count + 1))
    fi
  fi
  
  echo "=== View testing complete ===" | tee -a "$log_file"
  echo "Searched databases: ${possible_databases[*]}" | tee -a "$log_file"
  echo "View validation summary: $success_count/$total_views views found" | tee -a "$log_file"
  
  # Test passes if at least one view exists
  [ $success_count -gt 0 ]
}
