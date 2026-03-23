#!/bin/bash
set -e

# Terraform Test Local Run Wrapper Script
# This script orchestrates the complete CID testing workflow

echo "=== CID Terraform Test Local Run ==="
echo "Starting complete test workflow..."

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Set default environment variables if not provided
export ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
export RESOURCE_PREFIX="${RESOURCE_PREFIX:-cid-tf}"
export BACKEND_TYPE="${BACKEND_TYPE:-local}"
export DEFAULT_REGION=$(aws configure get region)
export S3_REGION="${S3_REGION:-$DEFAULT_REGION}"

# Local development options (set to true/false)
export BUILD_LOCAL_LAYER="${BUILD_LOCAL_LAYER:-true}"
export LOCAL_ASSETS_BUCKET_PREFIX="${LOCAL_ASSETS_BUCKET_PREFIX:-cid-${ACCOUNT_ID}-test}"
export LAYER_PREFIX="${LAYER_PREFIX:-cid-resource-lambda-layer}"
export TEMPLATE_PREFIX="${TEMPLATE_PREFIX:-cid-testing/templates}"
export FULL_BUCKET_NAME="$LOCAL_ASSETS_BUCKET_PREFIX-$S3_REGION"
export CID_VERSION=$(python3 -c "from cid import _version;print(_version.__version__)")


# Function to ensure S3 bucket exists, create if not
ensure_bucket_exists() {
    local bucket_name="$1"

    if [ -z "$bucket_name" ]; then
        echo "Error: Bucket name required"
        return 1
    fi

    # Check if bucket exists
    if aws s3api head-bucket --bucket "$bucket_name" 2>/dev/null; then
        echo "Bucket '$bucket_name' exists."
        return 0
    fi

    # Extract region (match AWS region pattern at end of bucket name)
    if [[ "$bucket_name" =~ ([a-z]{2}-[a-z]+-[0-9]+)$ ]]; then
        local aws_region="${BASH_REMATCH[1]}"
    else
        echo "suffix must be region"
        return 1
    fi

    echo "Creating bucket '$bucket_name'..."
    if [ "$aws_region" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "$bucket_name"
    else
        aws s3api create-bucket \
            --bucket "$bucket_name" \
            --region "$aws_region" \
            --create-bucket-configuration LocationConstraint="$aws_region"
    fi
}

echo "Configuration:"
echo "- Resource Prefix: $RESOURCE_PREFIX"
echo "- Backend Type: $BACKEND_TYPE"
echo "- AWS Region: $S3_REGION"
echo "- Build Local Layer: $BUILD_LOCAL_LAYER"
echo "- Local Assets Bucket: $FULL_BUCKET_NAME"
echo "- Layer Prefix: $LAYER_PREFIX"
echo "- Template Prefix: $TEMPLATE_PREFIX"


export quicksight_user=$(aws quicksight list-users \
        --aws-account-id $ACCOUNT_ID \
        --region $S3_REGION \
        --namespace default \
        --query "UserList[0].UserName" \
        --output text
)

if [ -z "$quicksight_user" ]; then
    echo "Cannot find QS user in ${ACCOUNT_ID} $S3_REGION"
    exit 1
fi

echo "Generating terraform.tfvars..."

# Extract dashboard values from user-config.tf defaults
USER_CONFIG_FILE="$PROJECT_ROOT/terraform/cicd-deployment/user-config.tf"
CUDOS_V5=$(awk '/variable "dashboards"/,/^}/ { if (/cudos_v5.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "yes")
COST_INTEL=$(awk '/variable "dashboards"/,/^}/ { if (/cost_intelligence.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
KPI_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/kpi.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
TRENDS_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/trends.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
DATATRANS_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/datatransfer.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
MARKET_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/marketplace.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
CONNECT_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/connect.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")
CONTAINERS_DASH=$(awk '/variable "dashboards"/,/^}/ { if (/containers.*= *"/) { gsub(/.*= *"/, ""); gsub(/".*/, ""); print; exit } }' "$USER_CONFIG_FILE" || echo "no")

# Extract CID CFN version from local cid-cfn.yml file
CID_CFN_VERSION=$(sed -n '3p' "$PROJECT_ROOT/cfn-templates/cid-cfn.yml" | grep -o 'v[0-9]\+\.[0-9]\+\.[0-9]\+' | sed 's/v//' || echo "${CID_VERSION}")

# Get latest data export version from external repo
echo "Fetching latest data export version..."
DATA_EXPORT_VER=$(curl -s https://raw.githubusercontent.com/aws-solutions-library-samples/cloud-intelligence-dashboards-data-collection/main/data-exports/deploy/data-exports-aggregation.yaml | grep Description | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "0.5.0")
ENVIRONMENT="dev"

echo "
# Configuration for one account deployment (not recommended for production)
global_values = {
  destination_account_id = \"${ACCOUNT_ID}\"        # Your AWS account ID
  source_account_ids     = \"${ACCOUNT_ID}\"        # Same account ID for local testing
  aws_region             = \"${S3_REGION}\"         # Your preferred region
  quicksight_user        = \"${quicksight_user}\"   # Your QuickSight username
  cid_cfn_version        = \"${CID_CFN_VERSION}\"   # CID CloudFormation version (from local file)
  data_export_version    = \"${DATA_EXPORT_VER}\"   # Data Export version
  environment            = \"${ENVIRONMENT}\"
}

# Dashboard configuration (extracted from user-config.tf defaults)
dashboards = {
  # Foundational
  cudos_v5          = \"${CUDOS_V5}\"
  cost_intelligence = \"${COST_INTEL}\"
  kpi               = \"${KPI_DASH}\"
  
  # Additional
  trends       = \"${TRENDS_DASH}\"
  datatransfer = \"${DATATRANS_DASH}\"
  marketplace  = \"${MARKET_DASH}\"
  connect      = \"${CONNECT_DASH}\"
  containers   = \"${CONTAINERS_DASH}\"
}
" | tee $PROJECT_ROOT/terraform/cicd-deployment/terraform.tfvars

# Optional: Build and upload local lambda layer for testing
if [ "${BUILD_LOCAL_LAYER:-false}" = "true" ]; then
  echo ""
  echo "=== Building Local Lambda Layer ==="

  # Check if we're in the right directory
  if [ -f "$PROJECT_ROOT/assets/build_lambda_layer.sh" ]; then
    cd "$PROJECT_ROOT"
    echo "Building lambda layer from local code..."
    LAYER_ZIP=$(./assets/build_lambda_layer.sh)

    if [ ! -z "$LAYER_ZIP" ] && [ -f "$LAYER_ZIP" ]; then
      echo "Built layer: $LAYER_ZIP"

      # Upload to your test bucket if specified
      if [ ! -z "${LOCAL_ASSETS_BUCKET_PREFIX}" ]; then
        # CloudFormation expects bucket name with region suffix
        FULL_BUCKET_NAME="$LOCAL_ASSETS_BUCKET_PREFIX-$S3_REGION"
        ensure_bucket_exists $FULL_BUCKET_NAME

        echo "Uploading layer to assets bucket: $FULL_BUCKET_NAME/$LAYER_PREFIX"
        aws s3 cp "$LAYER_ZIP" "s3://$FULL_BUCKET_NAME/$LAYER_PREFIX/$LAYER_ZIP"

        # Set environment variable for deploy script to use local layer bucket
        export LOCAL_LAYER_BUCKET="$LOCAL_ASSETS_BUCKET_PREFIX"
      else
        echo "Layer built locally: $(pwd)/$LAYER_ZIP"
        echo "Set LOCAL_ASSETS_BUCKET_PREFIX environment variable to upload to S3"
      fi
    else
      echo "Error: Failed to build lambda layer"
    fi
    
    cd "$PROJECT_ROOT"
  else
    echo "Error: build_lambda_layer.sh not found at $PROJECT_ROOT/assets/"
  fi
fi

# Step 1.5: Check for existing stacks and handle cleanup
echo ""
echo "=== Checking for Existing CloudFormation Stacks ==="
echo "Scanning for CID stacks (foundational + additional dashboards)..."

# Check for existing CloudFormation stacks
# Foundational stacks
FOUNDATIONAL_STACKS=("CID-DataExports-Destination" "Cloud-Intelligence-Dashboards")
# Additional CUR-based dashboard stacks
ADDITIONAL_STACKS=("Trends-Dashboard" "DataTransfer-Cost-Analysis-Dashboard" "AWS-Marketplace-SPG-Dashboard" "Amazon-Connect-Cost-Insight-Dashboard" "SCAD-Containers-Cost-Allocation-Dashboard")
# All stacks to check (excluding optional advanced stacks to reduce noise)
STACKS_TO_CHECK=("${FOUNDATIONAL_STACKS[@]}" "${ADDITIONAL_STACKS[@]}")
GOOD_STATES=("CREATE_COMPLETE" "UPDATE_COMPLETE")
FAILED_STATES=("CREATE_FAILED" "ROLLBACK_COMPLETE" "ROLLBACK_FAILED" "DELETE_FAILED" "UPDATE_ROLLBACK_COMPLETE" "UPDATE_ROLLBACK_FAILED" "IMPORT_ROLLBACK_COMPLETE" "IMPORT_ROLLBACK_FAILED")
IN_PROGRESS_STATES=("CREATE_IN_PROGRESS" "DELETE_IN_PROGRESS" "UPDATE_IN_PROGRESS" "UPDATE_ROLLBACK_IN_PROGRESS" "REVIEW_IN_PROGRESS" "IMPORT_IN_PROGRESS" "IMPORT_ROLLBACK_IN_PROGRESS")

FOUND_STACKS=()
GOOD_STACKS=()
FAILED_STACKS=()
IN_PROGRESS_STACKS=()

# Check foundational stacks first
echo "Checking foundational stacks:"
for stack in "${FOUNDATIONAL_STACKS[@]}"; do
  if STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$S3_REGION" --query 'Stacks[0].StackStatus' --output text 2>/dev/null); then
    echo "  ✓ Found: $stack (Status: $STACK_STATUS)"
    FOUND_STACKS+=("$stack")
    
    if [[ " ${GOOD_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      GOOD_STACKS+=("$stack")
    elif [[ " ${FAILED_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      FAILED_STACKS+=("$stack")
    elif [[ " ${IN_PROGRESS_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      IN_PROGRESS_STACKS+=("$stack")
    fi
  else
    echo "  - Not found: $stack"
  fi
done

# Check additional dashboard stacks
echo "Checking additional CUR-based dashboard stacks:"
for stack in "${ADDITIONAL_STACKS[@]}"; do
  if STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$S3_REGION" --query 'Stacks[0].StackStatus' --output text 2>/dev/null); then
    echo "  ✓ Found: $stack (Status: $STACK_STATUS)"
    FOUND_STACKS+=("$stack")
    
    if [[ " ${GOOD_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      GOOD_STACKS+=("$stack")
    elif [[ " ${FAILED_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      FAILED_STACKS+=("$stack")
    elif [[ " ${IN_PROGRESS_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      IN_PROGRESS_STACKS+=("$stack")
    fi
  else
    echo "  - Not found: $stack"
  fi
done

echo ""
echo "Stack Summary:"
echo "- Total stacks found: ${#FOUND_STACKS[@]}"
echo "- Stacks in good state: ${#GOOD_STACKS[@]}"
echo "- Stacks in failed state: ${#FAILED_STACKS[@]}"
echo "- Stacks in progress: ${#IN_PROGRESS_STACKS[@]}"

if [ ${#FOUND_STACKS[@]} -gt 0 ]; then
  # Handle in-progress stacks first
  if [ ${#IN_PROGRESS_STACKS[@]} -gt 0 ]; then
    echo ""
    echo "Found stacks in progress states:"
    for stack in "${IN_PROGRESS_STACKS[@]}"; do
      echo "  - $stack"
    done
    echo "Cannot proceed while stacks are in progress. Please wait for completion or cancel operations."
    exit 1
  # Auto-cleanup if any stacks are in failed states
  elif [ ${#FAILED_STACKS[@]} -gt 0 ]; then
    echo ""
    echo "Found stacks in failed states. Auto-cleaning up:"
    for stack in "${FAILED_STACKS[@]}"; do
      echo "  - $stack"
    done
    echo "Running cleanup..."
    bash "$SCRIPT_DIR/cleanup.sh"
    echo "Cleanup completed. Proceeding with fresh deployment..."
  # Auto-cleanup if partial foundational deployment (foundational stacks incomplete)
  elif [ ${#GOOD_STACKS[@]} -gt 0 ]; then
    # Check if foundational stacks are complete
    FOUNDATIONAL_GOOD=0
    for stack in "${FOUNDATIONAL_STACKS[@]}"; do
      if [[ " ${GOOD_STACKS[*]} " =~ " ${stack} " ]]; then
        FOUNDATIONAL_GOOD=$((FOUNDATIONAL_GOOD + 1))
      fi
    done
    
    if [ $FOUNDATIONAL_GOOD -lt ${#FOUNDATIONAL_STACKS[@]} ]; then
      echo ""
      echo "Found partial foundational deployment (only $FOUNDATIONAL_GOOD of ${#FOUNDATIONAL_STACKS[@]} foundational stacks in good state)."
      echo "Auto-cleaning up for fresh deployment..."
      bash "$SCRIPT_DIR/cleanup.sh"
      echo "Cleanup completed. Proceeding with fresh deployment..."
    else
      echo ""
      echo "Found deployment with ${#GOOD_STACKS[@]} total stacks:"
      echo "Foundational stacks:"
      for stack in "${FOUNDATIONAL_STACKS[@]}"; do
        if [[ " ${GOOD_STACKS[*]} " =~ " ${stack} " ]]; then
          echo "  ✓ $stack"
        fi
      done
      echo "Additional dashboard stacks:"
      for stack in "${ADDITIONAL_STACKS[@]}"; do
        if [[ " ${GOOD_STACKS[*]} " =~ " ${stack} " ]]; then
          echo "  ✓ $stack"
        fi
      done
      echo ""
      read -p "Do you want to clean up existing deployment before proceeding? (y/N): " -n 1 -r
      echo
      if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Running cleanup..."
        bash "$SCRIPT_DIR/cleanup.sh"
        echo "Cleanup completed. Proceeding with fresh deployment..."
      else
        echo "Keeping existing deployment. Proceeding with current state..."
      fi
    fi
  fi
else
  echo "No existing stacks found. Proceeding with fresh deployment."
fi

# Step 2: Run deployment
echo ""
echo "=== Step 2: Running Deployment ==="
bash "$SCRIPT_DIR/deploy.sh"

# Step 3: Dashboard Validation
echo ""
echo "=== Step 3: Dashboard Validation ==="

# Run foundational dashboard tests
if [ -f "$SCRIPT_DIR/.temp_dir" ]; then
  TEMP_DIR=$(cat "$SCRIPT_DIR/.temp_dir")
  if [ -d "$TEMP_DIR" ]; then
    echo "Running foundational dashboard checks..."
    bash "$SCRIPT_DIR/check_dashboards.sh" "$TEMP_DIR"
  else
    echo "Error: Temp directory not found, cannot run dashboard checks"
    exit 1
  fi
else
  echo "Error: No temp directory reference found, cannot run dashboard checks"
  exit 1
fi

# Run additional dashboard validation
echo ""
echo "=== Additional Dashboards Validation ==="
echo "Checking for additional dashboards..."

# Check each additional dashboard
for dashboard_id in trends-dashboard datatransfer-cost-analysis-dashboard aws-marketplace amazon-connect-cost-insight-dashboard scad-containers-cost-allocation; do
  case $dashboard_id in
    trends-dashboard) view_name="daily_anomaly_detection" ;;
    datatransfer-cost-analysis-dashboard) view_name="data_transfer_view" ;;
    aws-marketplace) view_name="marketplace_view" ;;
    amazon-connect-cost-insight-dashboard) view_name="resource_connect_view" ;;
    scad-containers-cost-allocation) view_name="scad_cca_summary_view" ;;
  esac
  
  echo "Checking dashboard: $dashboard_id"
  if aws quicksight describe-dashboard \
      --aws-account-id "$ACCOUNT_ID" \
      --dashboard-id "$dashboard_id" \
      --output json > /dev/null 2>&1; then
    echo "✓ Dashboard $dashboard_id exists"
    
    # Check associated view
    if aws athena get-table-metadata \
        --catalog-name 'AwsDataCatalog' \
        --database-name 'cid_cur' \
        --table-name "$view_name" \
        --output json > /dev/null 2>&1; then
      echo "✓ View $view_name exists"
    else
      echo "⚠ View $view_name not found (may not be created yet)"
    fi
  else
    echo "- Dashboard $dashboard_id not deployed"
  fi
done

# Step 4: Cleanup
echo ""
echo "=== Step 4: Cleanup ==="
echo "The deployment and testing is complete."
echo "Do you want to clean up the deployed resources? This will:"
echo "- Empty S3 data buckets"
echo "- Run 'terraform destroy' to remove all CID resources"
echo "- Clean up temporary files"
echo ""
read -p "Proceed with cleanup? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting cleanup..."
    bash "$SCRIPT_DIR/cleanup.sh"
else
    echo "Cleanup skipped. Resources remain deployed."
    echo "To clean up later, run: bash $SCRIPT_DIR/cleanup.sh"
fi

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "=== Test Workflow Completed Successfully ==="
    echo "Summary:"
    echo "- Foundational dashboards: Validated"
    echo "- Additional dashboards: Checked"
    echo "- Resources remain deployed for further testing"
else
    echo ""
    echo "=== Cleanup Completed Successfully ==="
    echo "All CID resources have been removed from your account."
fi