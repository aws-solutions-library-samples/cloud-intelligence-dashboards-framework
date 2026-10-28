#!/bin/bash
set -e

export AWS_PAGER=""

# Dashboard checking script
# This script extracts dashboard settings from Terraform configuration and runs tests

echo "=== Dashboard Configuration Check ==="

# Ensure AWS region is set for QuickSight API calls
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${S3_REGION:-$(aws configure get region)}}"
export AWS_REGION="$AWS_DEFAULT_REGION"

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
focus="no"
cora="no"
pca="no"
msih="no"

# Prefer terraform.tfvars when it sets dashboards (that is what Terraform deploys),
# otherwise fall back to the defaults in user-config.tf
CONFIG_FILE=""
if [ -f "terraform.tfvars" ] && grep -qE '^[[:space:]]*dashboards[[:space:]]*=' "terraform.tfvars"; then
  CONFIG_FILE="terraform.tfvars"
  echo "Found dashboards in terraform.tfvars, extracting dashboard settings..."
elif [ -f "user-config.tf" ]; then
  CONFIG_FILE="user-config.tf"
  echo "Found user-config.tf, extracting dashboard settings..."
fi

# Print the value of the first `<key> = "<value>"` assignment, or the default if there is none.
# Matching the whole assignment skips type lines (`key = string`, `key = optional(string, "no")`)
# and validation lines (`var.dashboards.key == "yes"`).
get_dashboard_value() {
  local key="$1" default="$2" value
  value=$(grep -E "^[[:space:]]*${key}[[:space:]]*=[[:space:]]*\"" "$CONFIG_FILE" | head -1 | cut -d'"' -f2)
  echo "${value:-$default}"
}

if [ -n "$CONFIG_FILE" ]; then
  # Extract foundational dashboards
  deploy_cudos_v5=$(get_dashboard_value cudos_v5 "$deploy_cudos_v5")
  deploy_cost_intelligence_dashboard=$(get_dashboard_value cost_intelligence "$deploy_cost_intelligence_dashboard")
  deploy_kpi_dashboard=$(get_dashboard_value kpi "$deploy_kpi_dashboard")

  # Extract additional dashboards
  trends=$(get_dashboard_value trends "$trends")
  datatransfer=$(get_dashboard_value datatransfer "$datatransfer")
  marketplace=$(get_dashboard_value marketplace "$marketplace")
  connect=$(get_dashboard_value connect "$connect")
  containers=$(get_dashboard_value containers "$containers")
  focus=$(get_dashboard_value focus "$focus")
  cora=$(get_dashboard_value cora "$cora")
  pca=$(get_dashboard_value pca "$pca")
  msih=$(get_dashboard_value msih "$msih")
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
export focus
export cora
export pca
export msih

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
echo "- focus: $focus"
echo "- cora: $cora"
echo "- pca: $pca"
echo "- msih: $msih"

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