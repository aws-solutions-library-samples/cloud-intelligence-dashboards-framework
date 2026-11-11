#!/bin/bash
set -e

export AWS_PAGER=""

# Configuration
BACKEND_TYPE=${BACKEND_TYPE:-"local"}  # Can be "local" or "s3"
S3_BUCKET=${S3_BUCKET:-""}
S3_KEY=${S3_KEY:-"terraform/cid-test/terraform.tfstate"}
S3_REGION=${S3_REGION:-$(aws configure get region)}  # Use configured region as default
BACKEND_REGION=${BACKEND_REGION:-"us-east-1"}  # Backend bucket region

# Set AWS region for AWS CLI commands
export AWS_DEFAULT_REGION="${S3_REGION}"
export AWS_REGION="${S3_REGION}"  # Some AWS tools use this variable instead

# Verify account access
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
PARTITION=$(aws sts get-caller-identity --query Arn --output text | cut -d: -f2)
echo "Account ID: $ACCOUNT_ID"

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Use existing directory or create temp directory based on execution mode
if [ -f "./terraform/cicd-deployment/backend.tf" ]; then
  echo "Found existing backend.tf, using existing directory (CI mode)"
  TEMP_DIR="./terraform/cicd-deployment"
else
  echo "No backend.tf found, creating temporary directory (standalone mode)"
  TEMP_DIR=$(mktemp -d)
  PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
  TERRAFORM_DIR="$PROJECT_ROOT/cicd-deployment"
  cp -r "$TERRAFORM_DIR"/* "$TEMP_DIR/"
  
  # Configure backend for standalone execution
  if [ "$BACKEND_TYPE" == "s3" ]; then
    echo "Configuring S3 backend..."
    cat > "$TEMP_DIR/backend.tf" << EOF
terraform {
  backend "s3" {
    bucket = "$S3_BUCKET"
    key    = "$S3_KEY"
    region = "$BACKEND_REGION"
  }
}
EOF
  else
    echo "Configuring local backend..."
    TFSTATE_DIR="$SCRIPT_DIR/tfstate"
    mkdir -p "$TFSTATE_DIR"
    cat > "$TEMP_DIR/backend.tf" << EOF
terraform {
  backend "local" {
    path = "$TFSTATE_DIR/terraform.tfstate"
  }
}
EOF
  fi
  
  # Create providers for standalone execution
  echo "Creating providers.tf with region ${S3_REGION}..."
  cat > "$TEMP_DIR/providers.tf" << EOF
provider "aws" {
  alias  = "management"
  region = "${S3_REGION}"
}

provider "aws" {
  alias  = "datacollection"
  region = "${S3_REGION}"
}
EOF
fi
echo "Using Terraform directory at $TEMP_DIR"

echo "Cleaning up resources..."
echo "Backend bucket (protected): $S3_BUCKET"

# Get resource prefix from environment variable or use default
RESOURCE_PREFIX=${RESOURCE_PREFIX:-"cid-tf"}

# Empty specific CID S3 buckets (but don't delete them)
echo "Emptying CID S3 buckets..."

# Define specific bucket patterns to clean (only data buckets)
CID_DATA_BUCKETS=(
  "${RESOURCE_PREFIX}-${ACCOUNT_ID}-data-exports"
  "${RESOURCE_PREFIX}-${ACCOUNT_ID}-data-local"
  # Must match MyAthenaQueryResultsBucket in cfn-templates/cid-cfn.yml. This bucket has
  # DeletionPolicy: Delete, so if it is not empty the Cloud-Intelligence-Dashboards stack
  # ends in DELETE_FAILED and blocks every subsequent deployment.
  "${PARTITION}-athena-query-results-cid-${ACCOUNT_ID}-${S3_REGION}"
)

# CloudFormation stacks managed by terraform/cicd-deployment, in reverse dependency order
STACKS_DELETE_ORDER=(
  "CORA-Dashboard"
  "FOCUS-Dashboard"
  "SCAD-Containers-Dashboard"
  "Amazon-Connect-Cost-Insight-Dashboard"
  "AWS-Marketplace-SPG-Dashboard"
  "DataTransfer-Cost-Analysis-Dashboard"
  "Trends-Dashboard"
  "Cloud-Intelligence-Dashboards"
  "CID-DataExports-Source"
  "CID-DataExports-Destination"
)

stack_status() {
  aws cloudformation describe-stacks --stack-name "$1" \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND"
}

empty_bucket() {
  local bucket=$1
  aws s3api head-bucket --bucket "$bucket" 2>/dev/null || return 0
  aws s3 rm "s3://$bucket" --recursive || true
  # delete-objects accepts at most 1000 keys per call, so page through versions
  for attempt in {1..50}; do
    VERSIONS=$(aws s3api list-object-versions --bucket "$bucket" --max-items 1000 --output json 2>/dev/null || echo '{}')
    OBJECTS=$(echo "$VERSIONS" | jq -c '[(.Versions // [])[], (.DeleteMarkers // [])[] | {Key:.Key, VersionId:.VersionId}]')
    if [ "$(echo "$OBJECTS" | jq 'length')" -eq 0 ]; then
      echo "Bucket $bucket is empty"
      return 0
    fi
    echo "Deleting $(echo "$OBJECTS" | jq 'length') object versions from $bucket (batch $attempt)"
    echo "{\"Objects\": $OBJECTS, \"Quiet\": true}" | aws s3api delete-objects --bucket "$bucket" --delete file:///dev/stdin >/dev/null || true
  done
  echo "WARNING: $bucket still not empty after 50 batches"
}

# CloudFormation refuses to delete a stack while another stack imports one of its
# exports (e.g. cid-CidExecArn is imported by every cid-plugin dashboard stack). The
# delete is rejected and the stack goes back to CREATE_COMPLETE/UPDATE_COMPLETE, which
# terraform reports as "unexpected state 'CREATE_COMPLETE'". Importing stacks can exist
# outside terraform state (left over from earlier runs or other branches), so find
# them through the CloudFormation API and delete them first.
delete_importers() {
  local stack=$1 exports exp_name importers importer
  exports=$(aws cloudformation describe-stacks --stack-name "$stack" \
    --query 'Stacks[0].Outputs[?ExportName!=`null`].ExportName' --output text 2>/dev/null) || return 0
  for exp_name in $exports; do
    [ "$exp_name" = "None" ] && continue
    importers=$(aws cloudformation list-imports --export-name "$exp_name" \
      --query 'Imports' --output text 2>/dev/null) || continue  # errors when not imported
    for importer in $importers; do
      [ "$importer" = "None" ] && continue
      echo "Stack $importer imports export $exp_name from $stack. Deleting $importer first..."
      force_delete_stack "$importer"
    done
  done
}

# Delete a stack directly through CloudFormation. Used when terraform destroy fails
# or when state is out of sync with what exists in the account.
force_delete_stack() {
  local stack=$1
  local status
  status=$(stack_status "$stack")
  if [ "$status" = "NOT_FOUND" ] || [ "$status" = "DELETE_COMPLETE" ]; then
    return 0
  fi

  echo "Stack $stack still exists (status: $status). Deleting via CloudFormation API..."
  delete_importers "$stack"
  if [[ "$status" == *_IN_PROGRESS && "$status" != "DELETE_IN_PROGRESS" ]]; then
    echo "Waiting for in-progress operation on $stack to finish..."
    aws cloudformation wait stack-update-complete --stack-name "$stack" 2>/dev/null \
      || aws cloudformation wait stack-create-complete --stack-name "$stack" 2>/dev/null || true
  fi

  # Empty the CID buckets again in case anything was written since the first pass
  for bucket in "${CID_DATA_BUCKETS[@]}"; do empty_bucket "$bucket"; done

  aws cloudformation delete-stack --stack-name "$stack" || true
  if aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null; then
    return 0
  fi

  status=$(stack_status "$stack")
  if [ "$status" != "NOT_FOUND" ] && [ "$status" != "DELETE_FAILED" ]; then
    echo "Delete of $stack was rejected (status back to $status). Reason:"
    aws cloudformation describe-stack-events --stack-name "$stack" --max-items 20 \
      --query "StackEvents[?LogicalResourceId=='$stack' && ResourceStatusReason!=\`null\`].ResourceStatusReason" \
      --output text | head -3 || true
  fi
  if [ "$status" = "DELETE_FAILED" ]; then
    echo "Stack $stack is DELETE_FAILED. Resources that failed to delete:"
    aws cloudformation describe-stack-events --stack-name "$stack" \
      --query "StackEvents[?ResourceStatus=='DELETE_FAILED'].[LogicalResourceId,ResourceStatusReason]" \
      --output text | sort -u || true
    # Last resort for the test account: retain the failed resources so the stack
    # itself is removed and the next deployment is unblocked. Retained S3 buckets
    # are removed by the bucket sweep below / on the next run.
    RETAIN=$(aws cloudformation list-stack-resources --stack-name "$stack" \
      --query "StackResourceSummaries[?ResourceStatus=='DELETE_FAILED'].LogicalResourceId" --output text)
    if [ -n "$RETAIN" ]; then
      echo "Retrying delete of $stack, retaining: $RETAIN"
      # shellcheck disable=SC2086
      aws cloudformation delete-stack --stack-name "$stack" --retain-resources $RETAIN || true
      aws cloudformation wait stack-delete-complete --stack-name "$stack" 2>/dev/null || true
    fi
  fi
}

for bucket in "${CID_DATA_BUCKETS[@]}"; do
  echo "Checking if bucket $bucket exists..."
  if [ "$bucket" = "$S3_BUCKET" ]; then
    echo "Skipping backend bucket: $bucket"
    continue
  fi
  if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
    echo "Emptying and deleting bucket $bucket..."
    empty_bucket "$bucket"
    # Delete the bucket before terraform destroy to prevent CloudFormation conflicts
    aws s3 rb "s3://$bucket" --force || true
    echo "Deleted bucket: $bucket"
  else
    echo "Bucket $bucket does not exist or is not accessible"
  fi
done

# Remove every stack that imports an export from a CID stack before terraform destroy,
# whether or not terraform tracks it. Otherwise an untracked importer makes destroy fail
# with "unexpected state 'CREATE_COMPLETE'". Terraform's refresh drops stacks deleted here.
echo "Deleting stacks that import CID exports..."
for stack in "${STACKS_DELETE_ORDER[@]}"; do
  delete_importers "$stack"
done

# Run terraform destroy
cd "$TEMP_DIR"
echo "Running terraform destroy..."

# Initialize terraform if not already done (standalone mode)
if [ ! -d ".terraform" ]; then
  echo "Initializing Terraform..."
  terraform init -reconfigure
fi

# Use the override file if it exists
TFVARS_FILES=()
if [ -f "terraform.tfvars" ]; then
  TFVARS_FILES+=("-var-file=terraform.tfvars")
fi
if [ -f "terraform.tfvars.override" ]; then
  TFVARS_FILES+=("-var-file=terraform.tfvars.override")
fi

# Create a temporary tfvars file with the region
if [ -f "$TEMP_DIR/terraform.tfvars" ]; then
  cat > "$TEMP_DIR/region.auto.tfvars" << EOF
global_values = {
  destination_account_id = "$(grep -o 'destination_account_id[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
  source_account_ids     = "$(grep -o 'source_account_ids[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
  aws_region             = "${S3_REGION}"
  quicksight_user        = "$(grep -o 'quicksight_user[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
  cid_cfn_version        = "$(grep -o 'cid_cfn_version[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
  data_export_version    = "$(grep -o 'data_export_version[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
  environment            = "$(grep -o 'environment[[:space:]]*=[[:space:]]*"[^"]*"' "$TEMP_DIR/terraform.tfvars" | cut -d'"' -f2)"
}
EOF
fi

DESTROY_RC=0
terraform destroy "${TFVARS_FILES[@]}" -auto-approve || DESTROY_RC=$?
cd "$SCRIPT_DIR"
if [ "$DESTROY_RC" -ne 0 ]; then
  echo "WARNING: terraform destroy exited with $DESTROY_RC. Falling back to CloudFormation deletes."
fi

# Delete anything terraform did not remove (failed destroy or out-of-sync state)
for stack in "${STACKS_DELETE_ORDER[@]}"; do
  force_delete_stack "$stack"
done

# Post-destroy safety check: ensure CID buckets are truly gone
echo "Post-destroy safety check: verifying CID buckets are deleted..."
for bucket in "${CID_DATA_BUCKETS[@]}"; do
  if aws s3api head-bucket --bucket $bucket 2>/dev/null; then
    echo "WARNING: Bucket $bucket still exists after destroy. Force deleting..."
    empty_bucket "$bucket"
    aws s3 rb "s3://$bucket" --force || true
  fi
done

# Fail loudly if any stack survived, so the problem is visible in this job
# rather than surfacing as a confusing failure in the next PR's deployment.
REMAINING_STACKS=()
for stack in "${STACKS_DELETE_ORDER[@]}"; do
  status=$(stack_status "$stack")
  if [ "$status" != "NOT_FOUND" ] && [ "$status" != "DELETE_COMPLETE" ]; then
    REMAINING_STACKS+=("$stack ($status)")
  fi
done
if [ ${#REMAINING_STACKS[@]} -gt 0 ]; then
  echo "ERROR: stacks still present after cleanup:"
  printf '  - %s\n' "${REMAINING_STACKS[@]}"
  exit 1
fi

# Clean up temporary directory if created for standalone execution
if [[ "$TEMP_DIR" == /tmp/* ]]; then
  echo "Cleaning up temporary Terraform directory"
  rm -rf "$TEMP_DIR"
fi
rm -f "$SCRIPT_DIR/.temp_dir"

echo "Cleanup completed successfully."
