# CID Terraform Testing

## Overview

The testing script deployes Dashboards and Data Export Stacks, then it tests if assets are succesfuly created and finally destroyes the stack.
The script allows the complete test of python CloudFormation and Terraform components. Test takes about 7 minutes.  

## Script

### Usage

```bash
./terraform/terraform-test/tf-test-local-run.sh
```

## Workflow Steps

### 1. Local Asset Preparation
- Builds local lambda layer (if enabled)
- Uploads local CID template (if enabled)
- Sets up environment variables for deployment

### 2. Terraform Deployment
- Creates temporary Terraform configuration
- Modifies variables to use local assets
- Deploys CID infrastructure
- Skips source account stack for local testing

### 3. Dashboard Validation
- Extracts dashboard configuration from Terraform
- Runs BATS tests to validate deployments
- Checks QuickSight dashboard availability

### 4. Resource Cleanup
- Empties S3 data buckets (preserves buckets)
- Runs `terraform destroy`
- Cleans up temporary files
- Removes any remaining resources


## Prerequisites

### Required Tools
- AWS CLI configured with appropriate permissions
- Terraform >= 1.0.0
- BATS (Bash Automated Testing System)
- jq (JSON processor)
- Python 3.x (for lambda layer building)

### AWS Permissions
Your AWS credentials need permissions for:
- **CloudFormation** (stack create/delete/describe operations)
- **S3** (bucket create/delete, object upload/download, versioning, lifecycle)
- **Glue** (database/table/crawler create/delete/start operations)
- **Athena** (workgroup create/delete, query execution)
- **Lambda** (function create/delete, layer publish/delete)
- **QuickSight** (full access for dashboards, datasets, datasources, users)
- **IAM** (role/policy create/delete/attach, PassRole)
- **CUR** (Cost and Usage Report definition operations)
- **BCM Data Exports** (Billing and Cost Management data exports)
- **KMS** (key operations for encryption/decryption)
- **CloudWatch Logs** (log group/stream creation for Lambda functions)

**Note**: The test requires extensive permissions as it creates a complete CID infrastructure including data pipelines, dashboards, and associated AWS resources.
