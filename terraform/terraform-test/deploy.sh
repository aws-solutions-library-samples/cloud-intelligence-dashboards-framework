#!/bin/bash
set -e

# Verify account access
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account ID: $ACCOUNT_ID"

# Configuration
BACKEND_TYPE=${BACKEND_TYPE:-"local"}  # Can be "local" or "s3"
S3_BUCKET=${S3_BUCKET:-""}
S3_KEY=${S3_KEY:-"terraform/cid-test/terraform.tfstate"}
S3_REGION=${S3_REGION:-"eu-west-2"}  # Default to eu-west-2
BACKEND_REGION=${BACKEND_REGION:-"us-east-1"}  # Backend bucket region
TEMPLATE_PREFIX="${TEMPLATE_PREFIX:-cid-testing/templates}"
LOCAL_ASSETS_BUCKET_PREFIX="${LOCAL_ASSETS_BUCKET_PREFIX:-cid-${ACCOUNT_ID}-test}"

# Set AWS region for AWS CLI commands
export AWS_DEFAULT_REGION="${S3_REGION}"
export AWS_REGION="${S3_REGION}"  # Some AWS tools use this variable instead

# Create a temporary directory for modified Terraform files
TEMP_DIR=$(mktemp -d)
echo "Creating temporary Terraform directory at $TEMP_DIR"

# Copy Terraform files to temporary directory
cp -r ./terraform/cicd-deployment/* "$TEMP_DIR/"

# Modify variables.tf to use local lambda layer if specified
if [ ! -z "${LOCAL_ASSETS_BUCKET_PREFIX}" ]; then

  echo "=== Preparing Local CID Template ==="
  FULL_BUCKET_NAME="$LOCAL_ASSETS_BUCKET_PREFIX-$S3_REGION"
  echo "Uploading local cid-cfn.yml to: s3://$FULL_BUCKET_NAME/$TEMPLATE_PREFIX/"
  aws s3 cp "./cfn-templates/cid-cfn.yml" "s3://$FULL_BUCKET_NAME/$TEMPLATE_PREFIX/cid-cfn.yml"
  LOCAL_CID_TEMPLATE_URL="https://$FULL_BUCKET_NAME.s3.amazonaws.com/$TEMPLATE_PREFIX/cid-cfn.yml"

  echo "Modifying locals.tf to use local CID template: $LOCAL_CID_TEMPLATE_URL"
  sed -i.bak "s|cudos        = \"\${local.common_template_url_base}/\${var.global_values.cid_cfn_version}/cid-cfn.yml\"|cudos        = \"$LOCAL_CID_TEMPLATE_URL\"|g" "$TEMP_DIR/locals.tf"

  LAYER_ZIP=$(./assets/build_lambda_layer.sh)
  aws s3 cp "$LAYER_ZIP" "s3://$FULL_BUCKET_NAME/cid-resource-lambda-layer/$LAYER_ZIP"

  echo "Modifying variables.tf to use local lambda layer bucket: $LOCAL_ASSETS_BUCKET_PREFIX"
  sed -i.bak "s|lambda_layer_bucket_prefix           = \"aws-managed-cost-intelligence-dashboards\"|lambda_layer_bucket_prefix           = \"$LOCAL_ASSETS_BUCKET_PREFIX\"|g" "$TEMP_DIR/variables.tf"
fi



# Create a directory to store the tfstate file
SCRIPT_DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
TFSTATE_DIR="$SCRIPT_DIR/tfstate"
mkdir -p "$TFSTATE_DIR"

# Set default resource prefix if not provided
RESOURCE_PREFIX=${RESOURCE_PREFIX:-"cid-tf"}
echo "Using resource prefix: ${RESOURCE_PREFIX}"

# Modify variables.tf to change the default resource prefix
echo "Modifying variables.tf to use resource prefix: ${RESOURCE_PREFIX}"
if [ -f "$TEMP_DIR/variables.tf" ]; then
  # Update only the resource_prefix in cid_dataexports_destination default section
  sed -i.bak "s/resource_prefix  = \"cid\"/resource_prefix  = \"${RESOURCE_PREFIX}\"/g" "$TEMP_DIR/variables.tf"
  
  # Verify the change
  echo "Verifying change in variables.tf:"
  grep -A1 "resource_prefix  = " "$TEMP_DIR/variables.tf" | head -2
fi

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
    cat > "$TEMP_DIR/backend.tf" << EOF
terraform {
  backend "local" {
    path = "$TFSTATE_DIR/terraform.tfstate"
  }
}
EOF
  fi
fi

# Modify provider.tf to use the same account for both providers and set region
cat > "$TEMP_DIR/local_override.tf" << EOF
provider "aws" {
  region = "${S3_REGION}"
}

provider "aws" {
  alias  = "destination_account"
  region = "${S3_REGION}"
}

terraform {
  required_version = ">= 1.0.0"
}
EOF

# Debug region settings
echo "Using AWS region: ${S3_REGION}"
echo "AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}"

# First check if the file exists and what it contains
echo "Examining main.tf file..."
if [ -f "$TEMP_DIR/main.tf" ]; then
  # Create a backup of the original file
  cp "$TEMP_DIR/main.tf" "$TEMP_DIR/main.tf.original"
  
  # Use grep to find the resource and then use awk to comment it out
  grep -n "resource \"aws_cloudformation_stack\" \"cid_dataexports_source\"" "$TEMP_DIR/main.tf" | while read -r line; do
    line_num=$(echo "$line" | cut -d: -f1)
    echo "Found cid_dataexports_source at line $line_num"
    
    # Use awk to comment out the resource block
    awk -v line="$line_num" 'NR==line {print "# SKIPPED FOR LOCAL TESTING\n# " $0; next} {print}' "$TEMP_DIR/main.tf.original" > "$TEMP_DIR/main.tf.tmp"
    mv "$TEMP_DIR/main.tf.tmp" "$TEMP_DIR/main.tf"
    
    # Find the closing brace of the resource block
    end_line=$(tail -n +$line_num "$TEMP_DIR/main.tf" | grep -n "^}" | head -1 | cut -d: -f1)
    end_line=$((line_num + end_line - 1))
    echo "Resource block ends at line $end_line"
    
    # Comment out all lines in the resource block
    awk -v start="$line_num" -v end="$end_line" 'NR>start && NR<=end {print "# " $0; next} {print}' "$TEMP_DIR/main.tf" > "$TEMP_DIR/main.tf.tmp"
    mv "$TEMP_DIR/main.tf.tmp" "$TEMP_DIR/main.tf"
  done
  
  # Update dependencies
  sed -i.bak 's/aws_cloudformation_stack.cid_dataexports_source,/# aws_cloudformation_stack.cid_dataexports_source,/g' "$TEMP_DIR/main.tf"
else
  echo "Error: main.tf not found in $TEMP_DIR"
  exit 1
fi

# Check and modify outputs.tf to remove references to deleted resources
if [ -f "$TEMP_DIR/outputs.tf" ]; then
  echo "Modifying outputs.tf to remove references to deleted resources..."
  
  # Find and remove the output block for cid_dataexports_source_outputs
  grep -n "output \"cid_dataexports_source_outputs\"" "$TEMP_DIR/outputs.tf" | while read -r line; do
    line_num=$(echo "$line" | cut -d: -f1)
    echo "Found cid_dataexports_source_outputs at line $line_num"
    
    # Find the closing brace of the output block
    end_line=$(tail -n +$line_num "$TEMP_DIR/outputs.tf" | grep -n "^}" | head -1 | cut -d: -f1)
    end_line=$((line_num + end_line - 1))
    echo "Output block ends at line $end_line"
    
    # Remove the entire output block
    sed -i.bak "${line_num},${end_line}d" "$TEMP_DIR/outputs.tf"
  done
fi

# Deploy with Terraform
cd "$TEMP_DIR"
echo "Initializing Terraform..."
terraform init -reconfigure

# Use terraform.tfvars if it exists
TFVARS_FILES=()
if [ -f "terraform.tfvars" ]; then
  TFVARS_FILES+=("-var-file=terraform.tfvars")
fi

# Import existing resources if state file is empty and using local backend
if [ "$BACKEND_TYPE" == "local" ]; then
  if [ ! -f "$TFSTATE_DIR/terraform.tfstate" ] || [ ! -s "$TFSTATE_DIR/terraform.tfstate" ]; then
    echo "No existing state file found or it's empty. Creating new state..."
  else
    echo "Found existing state file. Refreshing state..."
    terraform refresh
  fi
fi

echo "Planning Terraform deployment..."
terraform plan "${TFVARS_FILES[@]}" -out=tfplan

echo "Applying Terraform plan..."
terraform apply -auto-approve tfplan

# Save the temp directory path for cleanup script
echo "$TEMP_DIR" > "$SCRIPT_DIR/.temp_dir"
echo "Deployment completed successfully."
