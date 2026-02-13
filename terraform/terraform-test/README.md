# CID Terraform Testing Framework

## Overview

This testing framework provides comprehensive validation for CID Terraform deployments. It deploys dashboards and data export stacks, validates successful creation of all components, and performs cleanup. The complete test cycle takes approximately 7 minutes and validates both CloudFormation and Terraform components.

## Quick Start

```bash
./terraform/terraform-test/tf-test-local-run.sh
```

## Test Workflow

### 1. Environment Setup
- Extracts configuration from `user-config.tf` or `terraform.tfvars`
- Builds local lambda layer (optional)
- Uploads local CID templates to test bucket
- Generates `terraform.tfvars` with dynamic values

### 2. Stack Validation & Cleanup
- Checks for existing CloudFormation stacks
- Auto-cleanup for failed/partial deployments
- Handles all stack states (CREATE_FAILED, ROLLBACK_COMPLETE, etc.)

### 3. Terraform Deployment
- Creates temporary Terraform workspace
- Modifies configuration for local testing
- Deploys complete CID infrastructure
- Skips source account stack for single-account testing

### 4. Dashboard Validation
- Validates QuickSight dashboards exist and are accessible
- Checks Athena views and datasets
- Tests foundational dashboards (CUDOS, Cost Intelligence, KPI)
- Validates additional dashboards (Trends, DataTransfer, Marketplace, Connect, Containers)
- Runs BATS test suite for comprehensive validation

### 5. Resource Cleanup
- Empties S3 data buckets (preserves bucket structure)
- Executes `terraform destroy` with proper variable files
- Removes temporary files and configurations
- Cleans up any remaining AWS resources

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RESOURCE_PREFIX` | `cid-tf` | Prefix for AWS resources |
| `BACKEND_TYPE` | `local` | Terraform backend (`local` or `s3`) |
| `S3_REGION` | AWS CLI region | AWS region for deployment |
| `BUILD_LOCAL_LAYER` | `true` | Build lambda layer from local code |
| `LOCAL_ASSETS_BUCKET_PREFIX` | `cid-{account}-test` | S3 bucket for local assets |

### Dashboard Configuration

The test automatically extracts dashboard settings from `user-config.tf` (or falls back to `terraform.tfvars`):

- **Foundational**: CUDOS v5, Cost Intelligence, KPI
- **Additional**: Trends, DataTransfer, Marketplace, Connect, Containers

## Prerequisites

### Required Tools
- **AWS CLI** - Configured with appropriate credentials
- **Terraform** >= 1.0.0
- **BATS** - Bash Automated Testing System
- **jq** - JSON processor for parsing AWS responses
- **Python 3.x** - For lambda layer building
- **curl** - For fetching external configurations

### AWS Permissions

Your AWS credentials require extensive permissions for complete infrastructure testing:

#### Core Services
- **CloudFormation**: Full stack lifecycle operations
- **S3**: Bucket/object management, versioning, lifecycle policies
- **IAM**: Role/policy management, PassRole permissions
- **Lambda**: Function and layer management
- **CloudWatch Logs**: Log group/stream operations

#### Data & Analytics
- **Glue**: Database, table, and crawler operations
- **Athena**: Workgroup management and query execution
- **QuickSight**: Full access for dashboards, datasets, datasources, users

#### Billing & Cost Management
- **Cost and Usage Reports**: CUR definition operations
- **BCM Data Exports**: Billing data export management
- **KMS**: Encryption/decryption operations

## Test Components

### Core Scripts

| Script | Purpose |
|--------|---------|
| `tf-test-local-run.sh` | Main orchestration script |
| `deploy.sh` | Terraform deployment logic |
| `cleanup.sh` | Resource cleanup and destruction |
| `check_dashboards.sh` | Dashboard validation |
| `dashboards.bats` | BATS test suite |

### Test Validation

#### Dashboard Tests
- QuickSight dashboard accessibility
- Dataset and datasource validation
- Athena view existence checks
- Cross-dashboard dependency validation

#### Infrastructure Tests
- CloudFormation stack status validation
- S3 bucket and object verification
- Lambda function deployment checks
- Glue catalog and table validation

## Advanced Usage

### Custom Configuration

```bash
# Use S3 backend
export BACKEND_TYPE="s3"
export S3_BUCKET="my-terraform-state-bucket"
export S3_KEY="cid-test/terraform.tfstate"

# Custom resource prefix
export RESOURCE_PREFIX="my-cid-test"

# Disable local layer building
export BUILD_LOCAL_LAYER="false"

./terraform/terraform-test/tf-test-local-run.sh
```

### Selective Testing

```bash
# Test specific dashboards by editing user-config.tf
# The test script will automatically detect your dashboard configuration
```

### Debugging

Test logs are saved to `/tmp/cudos_test/test_output.log` for detailed analysis:

```bash
# View test logs
cat /tmp/cudos_test/test_output.log

# Check specific dashboard validation
ls /tmp/cudos_test/dashboard_*.json
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure AWS credentials have all required permissions
2. **Region Mismatch**: Verify S3_REGION matches your AWS CLI configuration
3. **Stack In Progress**: Wait for existing operations to complete before testing
4. **QuickSight Access**: Ensure QuickSight is enabled and user exists

### Manual Cleanup

If automated cleanup fails:

```bash
# Manual resource cleanup
export RESOURCE_PREFIX="cid-tf"
export S3_REGION="us-east-1"
./terraform/terraform-test/cleanup.sh
```

## Integration Testing

This framework supports both local development testing and CI/CD pipeline integration. The GitHub Actions workflow uses the same core scripts with environment-specific configurations.

**Note**: This testing framework is designed for builders and internal development. It creates complete AWS infrastructure and requires extensive permissions.