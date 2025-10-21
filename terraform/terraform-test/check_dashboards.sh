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

# Extract values using grep and sed - improved patterns to better match the structure
deploy_cudos_v5=$(grep -A30 'cudos_dashboard' variables.tf | grep 'deploy_cudos_v5' | grep -o '"yes"' | tr -d '"' || echo "no")
deploy_cost_intelligence_dashboard=$(grep -A30 'cudos_dashboard' variables.tf | grep 'deploy_cost_intelligence_dashboard' | grep -o '"yes"' | tr -d '"' || echo "no")
deploy_kpi_dashboard=$(grep -A30 'cudos_dashboard' variables.tf | grep 'deploy_kpi_dashboard' | grep -o '"yes"' | tr -d '"' || echo "no")

# Check if there's a terraform.tfvars file that might override the defaults
if [ -f "terraform.tfvars" ]; then
  echo "Found terraform.tfvars, checking for overrides..."
  if grep -q "deploy_cudos_v5" terraform.tfvars; then
    deploy_cudos_v5=$(grep "deploy_cudos_v5" terraform.tfvars | cut -d'=' -f2 | tr -d ' "' || echo "$deploy_cudos_v5")
  fi
  if grep -q "deploy_cost_intelligence_dashboard" terraform.tfvars; then
    deploy_cost_intelligence_dashboard=$(grep "deploy_cost_intelligence_dashboard" terraform.tfvars | cut -d'=' -f2 | tr -d ' "' || echo "$deploy_cost_intelligence_dashboard")
  fi
  if grep -q "deploy_kpi_dashboard" terraform.tfvars; then
    deploy_kpi_dashboard=$(grep "deploy_kpi_dashboard" terraform.tfvars | cut -d'=' -f2 | tr -d ' "' || echo "$deploy_kpi_dashboard")
  fi
fi

# Export the variables
export deploy_cudos_v5
export deploy_cost_intelligence_dashboard
export deploy_kpi_dashboard

# Echo the dashboard settings
echo "Dashboard settings from Terraform configuration:"
echo "- cudos-v5: $deploy_cudos_v5"
echo "- cost_intelligence_dashboard: $deploy_cost_intelligence_dashboard"
echo "- kpi_dashboard: $deploy_kpi_dashboard"

cd "$PROJECT_ROOT"

# Set database name
export database_name="${DATABASE_NAME:-cid_data_export}"
echo "Using database name: $database_name"

# Run BATS tests
echo "Running dashboard tests..."
cd "$SCRIPT_DIR"
bats dashboards.bats
echo "=== Dashboard Test Results ==="
if [ -f "/tmp/cudos_test/test_output.log" ]; then
  cat "/tmp/cudos_test/test_output.log"
else
  echo "Test log file not found"
fi
echo "=== End of Test Results ==="

echo "Dashboard checks completed successfully."