#!/bin/bash
set -e

# Configuration
BACKEND_TYPE=${BACKEND_TYPE:-"local"}  # Can be "local" or "s3"
S3_BUCKET=${S3_BUCKET:-""}
S3_KEY=${S3_KEY:-"terraform/cid-test/terraform.tfstate"}
S3_REGION=${S3_REGION:-"eu-west-2"}  # Default to eu-west-2
BACKEND_REGION=${BACKEND_REGION:-"us-east-1"}  # Backend bucket region

# Set AWS region for AWS CLI commands
export AWS_DEFAULT_REGION="${S3_REGION}"
export AWS_REGION="${S3_REGION}"  # Some AWS tools use this variable instead

# Debug region settings
echo "Using AWS region: ${S3_REGION}"
echo "AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}"

# Verify account access
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $ACCOUNT_ID"

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Get the temp directory from the deploy script
if [ -f "$SCRIPT_DIR/.temp_dir" ]; then
  TEMP_DIR=$(cat "$SCRIPT_DIR/.temp_dir")
  if [ ! -d "$TEMP_DIR" ]; then
    echo "Temp directory $TEMP_DIR not found. Creating a new one..."
    TEMP_DIR=$(mktemp -d)
    cp -r "$TERRAFORM_DIR"/* "$TEMP_DIR/"
    
    # Configure backend based on environment variable
    if [ -f "$TEMP_DIR/backend.tf" ]; then
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
    fi
    
    # Initialize Terraform
    cd "$TEMP_DIR"
    terraform init -reconfigure
  fi
else
  echo "No temp directory found from deploy script. Creating a new one..."
  TEMP_DIR=$(mktemp -d)
  PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
  TERRAFORM_DIR="$PROJECT_ROOT/cicd-deployment"
  cp -r "$TERRAFORM_DIR"/* "$TEMP_DIR/"
  
  # Configure backend based on environment variable
  if [ -f "$TEMP_DIR/backend.tf" ]; then
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
  fi
  
  # Initialize Terraform
  cd "$TEMP_DIR"
  terraform init -reconfigure
fi

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
  "aws-athena-query-results-cid-${ACCOUNT_ID}"
)

for bucket in "${CID_DATA_BUCKETS[@]}"; do
  echo "Checking if bucket $bucket exists..."
  if aws s3api head-bucket --bucket $bucket 2>/dev/null; then
    echo "Emptying bucket $bucket (keeping bucket)..."
    
    # Skip if this is the backend bucket
    if [ "$bucket" = "$S3_BUCKET" ]; then
      echo "Skipping backend bucket: $bucket"
      continue
    fi
    
    # First delete all non-versioned objects
    aws s3 rm s3://$bucket --recursive || true
    
    # Loop to ensure all versioned objects are deleted
    for attempt in {1..3}; do
      echo "Attempt $attempt to delete all versions..."
      
      # Get all versions and delete markers
      VERSIONS=$(aws s3api list-object-versions --bucket $bucket --output json 2>/dev/null || echo '{"Versions":[],"DeleteMarkers":[]}')
      
      # Process versions
      VERSION_COUNT=$(echo "$VERSIONS" | jq -r '.Versions | length // 0')
      VERSION_COUNT=${VERSION_COUNT:-0}
      
      if [ "$VERSION_COUNT" -gt 0 ]; then
        echo "Found $VERSION_COUNT versions to delete"
        echo "$VERSIONS" | jq -c '{Objects: [.Versions[] | {Key:.Key, VersionId:.VersionId}] | select(length > 0)}' | aws s3api delete-objects --bucket $bucket --delete file:///dev/stdin || true
      fi
      
      # Process delete markers
      MARKER_COUNT=$(echo "$VERSIONS" | jq -r '.DeleteMarkers | length // 0')
      MARKER_COUNT=${MARKER_COUNT:-0}
      
      if [ "$MARKER_COUNT" -gt 0 ]; then
        echo "Found $MARKER_COUNT delete markers to remove"
        echo "$VERSIONS" | jq -c '{Objects: [.DeleteMarkers[] | {Key:.Key, VersionId:.VersionId}] | select(length > 0)}' | aws s3api delete-objects --bucket $bucket --delete file:///dev/stdin || true
      fi
      
      # Check if bucket is empty
      REMAINING=$(aws s3api list-object-versions --bucket $bucket --output json 2>/dev/null || echo '{"Versions":[],"DeleteMarkers":[]}')
      VERSION_COUNT=$(echo "$REMAINING" | jq -r '.Versions | length // 0')
      MARKER_COUNT=$(echo "$REMAINING" | jq -r '.DeleteMarkers | length // 0')
      
      VERSION_COUNT=${VERSION_COUNT:-0}
      MARKER_COUNT=${MARKER_COUNT:-0}
      
      if [ "$VERSION_COUNT" -eq 0 ] && [ "$MARKER_COUNT" -eq 0 ]; then
        echo "Bucket $bucket is now empty (bucket preserved)!"
        break
      fi
      
      echo "Bucket still has objects, continuing..."
    done
  else
    echo "Bucket $bucket does not exist or is not accessible"
  fi
done

# Run terraform destroy
cd "$TEMP_DIR"
echo "Running terraform destroy..."

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

terraform destroy "${TFVARS_FILES[@]}" -auto-approve
cd "$SCRIPT_DIR"

# Final cleanup: Delete the data-exports bucket if it still exists
echo "Checking for remaining data-exports bucket..."
DATA_EXPORTS_BUCKET="${RESOURCE_PREFIX}-${ACCOUNT_ID}-data-exports"
if aws s3api head-bucket --bucket $DATA_EXPORTS_BUCKET 2>/dev/null; then
  echo "Found remaining bucket: $DATA_EXPORTS_BUCKET - deleting it..."
  # Ensure it's empty first
  aws s3 rm s3://$DATA_EXPORTS_BUCKET --recursive || true
  # Delete the bucket
  aws s3 rb s3://$DATA_EXPORTS_BUCKET --force || true
  echo "Deleted bucket: $DATA_EXPORTS_BUCKET"
else
  echo "Data-exports bucket already deleted or doesn't exist"
fi

# Clean up temporary directory
echo "Cleaning up temporary Terraform directory"
rm -rf "$TEMP_DIR"
rm -f "$SCRIPT_DIR/.temp_dir"

echo "Cleanup completed successfully."