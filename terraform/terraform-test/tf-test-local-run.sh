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

# Debug region consistency
echo ""
echo "=== Region Debug Info ==="
echo "Current AWS CLI region: $(aws configure get region)"
echo "AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION:-not set}"
echo "AWS_REGION: ${AWS_REGION:-not set}"
echo "S3_REGION: $S3_REGION"
echo "Account ID: $(aws sts get-caller-identity --query Account --output text)"


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

echo "
# Configuration for one account deployment (not recommended for production)
global_values = {
  destination_account_id = \"${ACCOUNT_ID}\"        # Your AWS account ID
  source_account_ids     = \"${ACCOUNT_ID}\"        # Same account ID for local testing
  aws_region             = \"${S3_REGION}\"         # Your preferred region
  quicksight_user        = \"${quicksight_user}\"   # Your QuickSight username
  cid_cfn_version        = \"${CID_VERSION}\"       # CID CloudFormation version # Will it be local that is used?
  data_export_version    = \"0.5.0\"                # Data Export version
  environment            = \"dev\"
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

# Check for existing CloudFormation stacks (single-account deployment)
STACKS_TO_CHECK=("CID-DataExports-Destination" "Cloud-Intelligence-Dashboards")
GOOD_STATES=("CREATE_COMPLETE" "UPDATE_COMPLETE")
FAILED_STATES=("CREATE_FAILED" "ROLLBACK_COMPLETE" "ROLLBACK_FAILED" "DELETE_FAILED" "UPDATE_ROLLBACK_COMPLETE" "UPDATE_ROLLBACK_FAILED" "IMPORT_ROLLBACK_COMPLETE" "IMPORT_ROLLBACK_FAILED")
IN_PROGRESS_STATES=("CREATE_IN_PROGRESS" "DELETE_IN_PROGRESS" "UPDATE_IN_PROGRESS" "UPDATE_ROLLBACK_IN_PROGRESS" "REVIEW_IN_PROGRESS" "IMPORT_IN_PROGRESS" "IMPORT_ROLLBACK_IN_PROGRESS")

FOUND_STACKS=()
GOOD_STACKS=()
FAILED_STACKS=()
IN_PROGRESS_STACKS=()

for stack in "${STACKS_TO_CHECK[@]}"; do
  if STACK_STATUS=$(aws cloudformation describe-stacks --stack-name "$stack" --region "$S3_REGION" --query 'Stacks[0].StackStatus' --output text 2>/dev/null); then
    echo "Found existing stack: $stack (Status: $STACK_STATUS)"
    FOUND_STACKS+=("$stack")
    
    if [[ " ${GOOD_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      GOOD_STACKS+=("$stack")
    elif [[ " ${FAILED_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      FAILED_STACKS+=("$stack")
    elif [[ " ${IN_PROGRESS_STATES[*]} " =~ " ${STACK_STATUS} " ]]; then
      IN_PROGRESS_STACKS+=("$stack")
    fi
  fi
done

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
  # Auto-cleanup if partial deployment (not all stacks in good state)
  elif [ ${#GOOD_STACKS[@]} -gt 0 ] && [ ${#GOOD_STACKS[@]} -lt ${#STACKS_TO_CHECK[@]} ]; then
    echo ""
    echo "Found partial deployment (only ${#GOOD_STACKS[@]} of ${#STACKS_TO_CHECK[@]} stacks in good state)."
    echo "Auto-cleaning up for fresh deployment..."
    bash "$SCRIPT_DIR/cleanup.sh"
    echo "Cleanup completed. Proceeding with fresh deployment..."
  # Ask for confirmation only if all stacks are in good state
  elif [ ${#GOOD_STACKS[@]} -eq ${#STACKS_TO_CHECK[@]} ]; then
    echo ""
    echo "Found complete deployment with all stacks in good state:"
    for stack in "${GOOD_STACKS[@]}"; do
      echo "  - $stack"
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
else
  echo "No existing stacks found. Proceeding with fresh deployment."
fi

# Step 2: Run deployment
echo ""
echo "=== Step 2: Running Deployment ==="
bash "$SCRIPT_DIR/deploy.sh"

# Step 3: Run dashboard checks
echo ""
echo "=== Step 3: Dashboard Validation ==="
if [ -f "$SCRIPT_DIR/.temp_dir" ]; then
  TEMP_DIR=$(cat "$SCRIPT_DIR/.temp_dir")
  if [ -d "$TEMP_DIR" ]; then
    echo "Running dashboard checks..."
    bash "$SCRIPT_DIR/check_dashboards.sh" "$TEMP_DIR"
  else
    echo "Error: Temp directory not found, cannot run dashboard checks"
    exit 1
  fi
else
  echo "Error: No temp directory reference found, cannot run dashboard checks"
  exit 1
fi

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

echo ""
echo "=== Test Workflow Completed Successfully ==="