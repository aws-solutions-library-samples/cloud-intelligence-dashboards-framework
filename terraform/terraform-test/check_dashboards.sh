#!/bin/bash
set -e

# Dashboard checking script
# This script extracts dashboard settings from Terraform configuration and runs tests

echo "=== Dashboard Configuration Check ==="

# Check if TEMP_DIR is provided as argument
if [ -z "$1" ]; then
  echo "Error: TEMP_DIR not provided"
  echo "Usage: $0 <TEMP_DIR>"
  exit 1
fi

TEMP_DIR="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Verify TEMP_DIR exists
if [ ! -d "$TEMP_DIR" ]; then
  echo "Error: TEMP_DIR $TEMP_DIR does not exist"
  exit 1
fi

# Extract dashboard variables directly from variables.tf file
cd "$TEMP_DIR"
echo "Extracting dashboard settings from Terraform configuration..."

# Extract foundational dashboard values from locals.tf
deploy_cudos_v5=$(grep -A10 'foundational.*=' locals.tf | grep 'cudos_v5' | grep -o '"yes"' | tr -d '"' || echo "no")
deploy_cost_intelligence_dashboard=$(grep -A10 'foundational.*=' locals.tf | grep 'cost_intelligence_dashboard' | grep -o '"yes"' | tr -d '"' || echo "no")
deploy_kpi_dashboard=$(grep -A10 'foundational.*=' locals.tf | grep 'kpi_dashboard' | grep -o '"yes"' | tr -d '"' || echo "no")

# Extract additional dashboard values from terraform.tfvars
deploy_trends_dashboard="no"
deploy_datatransfer_dashboard="no"
deploy_marketplace_dashboard="no"
deploy_connect_dashboard="no"
deploy_scad_containers_dashboard="no"

# Check terraform.tfvars for dashboard configuration
if [ -f "terraform.tfvars" ]; then
  echo "Found terraform.tfvars, extracting dashboard configuration..."
  
  # Extract dashboard values using awk for precise parsing
  deploy_cudos_v5=$(awk '/dashboards.*=.*{/,/^}/ { if (/cudos_v5.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_cost_intelligence_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/cost_intelligence_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_kpi_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/kpi_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_trends_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/trends_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_datatransfer_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/datatransfer_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_marketplace_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/marketplace_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_connect_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/connect_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
  deploy_scad_containers_dashboard=$(awk '/dashboards.*=.*{/,/^}/ { if (/scad_containers_dashboard.*=/) { gsub(/.*= *"|".*/, ""); print; exit } }' terraform.tfvars)
fi

# Export the variables
export deploy_cudos_v5
export deploy_cost_intelligence_dashboard
export deploy_kpi_dashboard
export deploy_trends_dashboard
export deploy_datatransfer_dashboard
export deploy_marketplace_dashboard
export deploy_connect_dashboard
export deploy_scad_containers_dashboard

# Echo the dashboard settings
echo "Dashboard settings from Terraform configuration:"
echo "Foundational Dashboards:"
echo "- cudos-v5: $deploy_cudos_v5"
echo "- cost_intelligence_dashboard: $deploy_cost_intelligence_dashboard"
echo "- kpi_dashboard: $deploy_kpi_dashboard"
echo "Additional Dashboards:"
echo "- trends_dashboard: $deploy_trends_dashboard"
echo "- datatransfer_dashboard: $deploy_datatransfer_dashboard"
echo "- marketplace_dashboard: $deploy_marketplace_dashboard"
echo "- connect_dashboard: $deploy_connect_dashboard"
echo "- scad_containers_dashboard: $deploy_scad_containers_dashboard"

cd "$PROJECT_ROOT"

# Set database name - prioritize cid_cur as it's where views are typically found
export database_name="${DATABASE_NAME:-cid_cur}"
echo "Using database name: $database_name"

# Check if any dashboards are enabled
if [ "$deploy_cudos_v5" = "no" ] && \
   [ "$deploy_cost_intelligence_dashboard" = "no" ] && \
   [ "$deploy_kpi_dashboard" = "no" ] && \
   [ "$deploy_trends_dashboard" = "no" ] && \
   [ "$deploy_datatransfer_dashboard" = "no" ] && \
   [ "$deploy_marketplace_dashboard" = "no" ] && \
   [ "$deploy_connect_dashboard" = "no" ] && \
   [ "$deploy_scad_containers_dashboard" = "no" ]; then
  echo "No dashboards enabled, skipping dashboard tests."
  echo "=== Dashboard Test Results ==="
  echo "All dashboards disabled - no tests to run"
  echo "=== End of Test Results ==="
  echo "Dashboard checks completed successfully."
  exit 0
fi

# Run BATS tests
echo "Running dashboard tests..."
cd "$SCRIPT_DIR"
bats dashboards.bats
echo "=== Dashboard Test Results ==="
if [ -f "/tmp/cudos_test/test_output.log" ]; then
  # Only show summary lines and section headers, not individual dataset checks
  grep -E "^(===|>>>|Dataset validation summary|View validation summary|Searched databases|✓ summary_view|✓ hourly_view|✓ resource_view|✓ s3_view|✓ ec2_running_cost|✓ kpi_|✓ daily_anomaly|✓ resource_connect)" "/tmp/cudos_test/test_output.log" || echo "No summary results found"
else
  echo "Test log file not found"
fi
echo "=== End of Test Results ==="

echo "Dashboard checks completed successfully."