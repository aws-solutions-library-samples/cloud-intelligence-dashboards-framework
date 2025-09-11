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

# Extract dashboard variables from terraform.tfvars
cd "$TEMP_DIR"
echo "Extracting dashboard settings from Terraform configuration..."

# Initialize defaults
deploy_cudos_v5="no"
deploy_cost_intelligence_dashboard="no"
deploy_kpi_dashboard="no"
trends="no"
datatransfer="no"
marketplace="no"
connect="no"
containers="no"

# Check terraform.tfvars for dashboard settings
if [ -f "terraform.tfvars" ]; then
  echo "Found terraform.tfvars, extracting dashboard settings..."
  
  # Extract foundational dashboards
  if grep -q "cudos_v5" terraform.tfvars; then
    deploy_cudos_v5=$(grep "cudos_v5" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "cost_intelligence" terraform.tfvars; then
    deploy_cost_intelligence_dashboard=$(grep "cost_intelligence" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "kpi" terraform.tfvars; then
    deploy_kpi_dashboard=$(grep "kpi" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  
  # Extract additional dashboards
  if grep -q "trends" terraform.tfvars; then
    trends=$(grep "trends" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "datatransfer" terraform.tfvars; then
    datatransfer=$(grep "datatransfer" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "marketplace" terraform.tfvars; then
    marketplace=$(grep "marketplace" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "connect" terraform.tfvars; then
    connect=$(grep "connect" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
  if grep -q "containers" terraform.tfvars; then
    containers=$(grep "containers" terraform.tfvars | grep -o '"[^"]*"' | tr -d '"' | head -1)
  fi
fi

# Export all variables
export deploy_cudos_v5
export deploy_cost_intelligence_dashboard
export deploy_kpi_dashboard
export trends
export datatransfer
export marketplace
export connect
export containers

# Echo the dashboard settings
echo "Dashboard settings from Terraform configuration:"
echo "Foundational:"
echo "- cudos_v5: $deploy_cudos_v5"
echo "- cost_intelligence: $deploy_cost_intelligence_dashboard"
echo "- kpi: $deploy_kpi_dashboard"
echo "Additional:"
echo "- trends: $trends"
echo "- datatransfer: $datatransfer"
echo "- marketplace: $marketplace"
echo "- connect: $connect"
echo "- containers: $containers"

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