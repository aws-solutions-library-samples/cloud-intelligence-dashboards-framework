# AWS Cloud Intelligence Dashboards (CID) Terraform Module

This Terraform module deploys the AWS Cloud Intelligence Dashboards (formerly CUDOS) infrastructure using CloudFormation stacks. It provides a streamlined way to set up cost management and optimization dashboards in your AWS environment.

## Architecture Overview

The module creates the following CloudFormation stacks across two AWS accounts:

1. **Data Exports Destination Stack** - Deployed in the Data Collection account to manage data aggregation
2. **Data Exports Source Stack** - Deployed in the Payer account to collect cost data
3. **Cloud Intelligence Dashboards Stack** - Deployed in the Data Collection account for QuickSight dashboards

This architecture follows AWS best practices by separating the Payer account (Source/Management account) from the dashboard visualization (Data Collection account). For a detailed architecture diagram, see the [CID Architecture Documentation](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html#architecture).

## Prerequisites

* Terraform >= 1.0
* Access to deploy resources in both accounts:
  * **Payer account**: Permissions to create IAM roles, S3 buckets, and access billing data
  * **Data Collection account**: Permissions to create CloudFormation stacks, S3 buckets, and manage QuickSight
* QuickSight Enterprise subscription in the Data Collection account
* A configured QuickSight user in the Data Collection account
* Terraform provider configuration for both accounts:
  * Provider with "management" alias for the Payer account
  * Provider with "datacollection" alias for the Data Collection account

## Quick Start

1. Configure your AWS credentials for both accounts
2. Edit `user-config.tf` with your configuration values
3. Create required `backend.tf` and `providers.tf` files
4. Run the standard Terraform workflow:

```bash
terraform init
terraform plan
terraform apply
```

## Configuration

### Required Configuration

**Edit `user-config.tf`** - This is the main file you need to modify:

```hcl
# Update these values for your environment
global_values = {
  destination_account_id = "123456789012"     # 12-digit Data Collection account ID
  source_account_ids     = "987654321098"     # Comma-separated list of Payer account IDs
  aws_region            = "us-east-1"         # AWS region for deployment
  quicksight_user       = "user@example.com" # QuickSight username
  cid_cfn_version       = "4.4.6"           # CID CloudFormation version
  data_export_version   = "0.9.0"           # Data Export version
  environment           = "prod"              # Environment (dev, staging, prod)
}

# Choose which dashboards to deploy
dashboards = {
  # Foundational (at least one required)
  cudos_v5          = "yes"  # CUDOS v5 Dashboard
  cost_intelligence = "no"  # Cost Intelligence Dashboard  
  kpi               = "no"  # KPI Dashboard
  
  # Additional CUR-based Dashboards
  trends       = "yes"  # Trends Dashboard
  datatransfer = "yes"  # Data Transfer Cost Analysis
  marketplace  = "yes"  # AWS Marketplace Dashboard
  connect      = "yes"  # Amazon Connect Cost Insight
  containers   = "yes"  # SCAD Containers Cost Allocation
}
```

> **Note:** To get the latest version numbers for `cid_cfn_version` and `data_export_version`, you can use the following commands:
>
> ```bash
> CID_VERSION=$(curl -s https://raw.githubusercontent.com/aws-solutions-library-samples/cloud-intelligence-dashboards-framework/main/cfn-templates/cid-cfn.yml | grep Description | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
> echo "cid_cfn_version = \"$CID_VERSION\""
>
> EXPORT_VERSION=$(curl -s https://raw.githubusercontent.com/aws-solutions-library-samples/cloud-intelligence-dashboards-data-collection/main/data-exports/deploy/data-exports-aggregation.yaml | grep Description | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1)
> echo "data_export_version = \"$EXPORT_VERSION\""
> ```

### Required Infrastructure Files

You need to create these files before deployment:

#### Create `backend.tf`
```hcl
terraform {
  backend "s3" {
    bucket = "your-terraform-state-bucket"
    key    = "cid-dashboards/terraform.tfstate"
    region = "us-east-1"
    encrypt = true
  }
}
```

#### Create `providers.tf`
```hcl
terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0.0"
    }
  }
}

provider "aws" {
  alias  = "management"
  region = var.global_values.aws_region
  # Payer account credentials
}

provider "aws" {
  alias  = "datacollection"
  region = var.global_values.aws_region
  # Data Collection account credentials
  assume_role {
    role_arn = "arn:aws:iam::${var.global_values.destination_account_id}:role/YourCrossAccountRole"
  }
}
```

## File Structure

- **`user-config.tf`** - 📝 **EDIT THIS** - Main configuration file for users
- **`variables.tf`** - ⚙️ **Advanced users only** - Technical/internal variables
- **`dashboards.tf`** - 🏗️ **Don't edit** - Resource definitions
- **`locals.tf`** - 🏗️ **Don't edit** - Internal logic and configurations
- **`outputs.tf`** - 📊 **Don't edit** - Output definitions
- **`backend.tf`** - 💾 **CREATE THIS** - Backend configuration
- **`providers.tf`** - 🔧 **CREATE THIS** - Provider configurations

## Cross-Account Setup

This module implements a cross-account architecture:

1. **Payer Account**: Contains the billing data and CUR reports
   * Deploys the Data Exports Source stack
   * Creates IAM roles for cross-account access
   * Sets up S3 bucket policies for data sharing

2. **Data Collection Account**: Contains the dashboards and visualization
   * Deploys the Data Exports Destination stack
   * Deploys the Cloud Intelligence Dashboards stack
   * Hosts the QuickSight dashboards and datasets

The cross-account setup ensures proper separation of concerns and follows AWS security best practices.

## Available Dashboards

The module can deploy the following dashboards in the Data Collection account:

### Foundational Dashboards
At least one foundational dashboard is required as they generate the CUR data needed by additional dashboards.

| Dashboard | Variable | Default | Description |
|-----------|----------|---------|-------------|
| CUDOS v5 | `cudos_v5` | yes | Comprehensive cost analysis |
| Cost Intelligence | `cost_intelligence` | yes | Advanced cost insights |
| KPI | `kpi` | yes | Key performance indicators |

### Additional CUR-based Dashboards
These require a foundational dashboard to be deployed first and use the same CUR data.

| Dashboard | Variable | Default | Description |
|-----------|----------|---------|-------------|
| Trends | `trends` | yes | Trends Dashboard |
| Data Transfer | `datatransfer` | yes | Data Transfer Cost Analysis |
| Marketplace | `marketplace` | yes | AWS Marketplace Dashboard |
| Connect | `connect` | yes | Amazon Connect Cost Insight |
| Containers | `containers` | yes | SCAD Containers Cost Allocation |

## Advanced Configuration

### Standalone Dashboard Deployments

For advanced use cases where you want to deploy individual dashboards without foundational dashboards:

```hcl
# Enable standalone deployment in user-config.tf
allow_standalone_dashboard = true

# Deploy only specific dashboards
dashboards = {
  # Disable foundational dashboards
  cudos_v5          = "no"
  cost_intelligence = "no"
  kpi               = "no"
  
  # Enable only the dashboard you want
  trends       = "yes"  # Deploy only Trends Dashboard
  datatransfer = "no"
  marketplace  = "no"
  connect      = "no"
  containers   = "no"
}
```

**Note**: This is typically used when foundational dashboards are already deployed in your environment and you want to add additional dashboards incrementally.

### Technical Parameters

If you need to modify technical parameters, you can edit `variables.tf`, but this is typically not needed for standard deployments. The following parameters are managed internally by the CID deployment and **must not be changed**:

| Parameter | Purpose |
|-----------|---------|
| `athena_workgroup` | Used by Athena to run queries |
| `athena_query_results_bucket` | Stores Athena query results for QuickSight |
| `database_name` | Athena/Glue database used by dashboards |
| `cur_table_name` | Name of the legacy CUR table if applicable |
| `suffix` | Unique stack identifier |
| `lambda_layer_bucket_prefix` | Bucket prefix for Lambda layers |
| `deployment_type` | Deployment mechanism (Terraform/CFN) |

## Outputs

After successful deployment, the module provides the following outputs:

| Output | Description |
|--------|-------------|
| `cid_dataexports_destination_outputs` | Outputs from destination stack |
| `cid_dataexports_source_outputs` | Outputs from source stack |
| `cloud_intelligence_dashboards_outputs` | QuickSight dashboard outputs |
| `dashboard_summary` | Summary of all deployed dashboards (foundational and additional) |
| `additional_dashboards_stacks` | Additional dashboard CloudFormation stacks (trends, datatransfer, marketplace, connect, containers) |

Access the dashboard URLs from the outputs to view your dashboards in QuickSight.

## Important Notes

* Stack creation can take up to 60 minutes
* All stacks require `CAPABILITY_IAM` and `CAPABILITY_NAMED_IAM` permissions
* The module uses S3 for Terraform state storage
* QuickSight Enterprise subscription is required in the Data Collection account
* Cross-account IAM roles are created automatically

## FAQ

<details>
<summary><b>How do I migrate from the non-modular configuration?</b></summary>

If you're upgrading from the previous non-modular Terraform configuration, please note the following breaking changes:

#### Configuration Structure Changes
- **Previous (Non-Modular)**: Configuration was scattered across multiple `.tf` files
- **New (Modular)**: Simplified modular structure with `user-config.tf` as the main configuration file

#### Required Actions for Migration

1. **Backup your existing configuration**:
   ```bash
   # Backup your old scattered .tf files
   cp *.tf backup/
   ```

2. **Configure the new `user-config.tf`** with your existing values:
   ```hcl
   # Edit user-config.tf with your existing values
   global_values = {
     destination_account_id = "your-account-id"     # From your old config
     source_account_ids     = "your-source-ids"     # From your old config
     aws_region            = "your-region"          # From your old config
     quicksight_user       = "your-qs-user"        # From your old config
     cid_cfn_version       = "4.4.6"               # Updated version
     data_export_version   = "0.9.0"               # Updated version
     environment           = "prod"                 # From your old config
   }
   
   dashboards = {
     # Foundational (at least one required)
     cudos_v5          = "yes"  # If you had CUDOS enabled
     cost_intelligence = "no"   # Based on your previous setup or desired to deploy
     kpi               = "no"   # Based on your previous setup or desired to deploy
     
     # Additional CUR-based Dashboards
     trends       = "no"   # Based on your previous setup or desired to deploy
     datatransfer = "no"   # Based on your previous setup or desired to deploy
     marketplace  = "no"   # Based on your previous setup or desired to deploy
     connect      = "no"   # Based on your previous setup or desired to deploy
     containers   = "no"   # Based on your previous setup or desired to deploy
   }
   ```

3. **Update your backend and providers** (if needed):
   - Ensure your `backend.tf` and `providers.tf` are compatible
   - Check provider version requirements

4. **Plan before applying**:
   ```bash
   terraform plan
   # Review changes carefully before applying
   terraform apply
   ```

</details>

<details>
<summary><b>How do I backfill historical cost data?</b></summary>

To backfill historical data for the Data Export, follow the instructions in the [CID Documentation Backfill Data Export section](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html#backfill-data-export).

This process allows you to populate your dashboards with historical cost and usage data, ensuring you have a complete view of your AWS spending over time.

</details>

<details>
<summary><b>Can I deploy everything in a single account instead of using cross-account setup?</b></summary>

While the cross-account setup is recommended for production environments, you can deploy the entire solution in your Payer account without requiring a separate Data Collection account. This single-account approach is simpler for testing or development purposes. To do this:

1. **Update user-config.tf**:
   ```hcl
   global_values = {
     destination_account_id = "123456789012"      # Your Payer account ID
     source_account_ids     = "123456789012"      # Same Payer account ID
     aws_region             = "us-east-1"         # AWS region for deployment
     quicksight_user        = "user/example"      # QuickSight username
     cid_cfn_version        = "4.4.6"             # CID CloudFormation version
     data_export_version    = "0.9.0"             # Data Export version
     environment            = "dev"               # Environment (dev, staging, prod)
   }
   ```

2. **Simplify providers.tf**:
   ```hcl
   provider "aws" {
     alias  = "management"
     region = var.global_values.aws_region
   }

   provider "aws" {
     alias  = "datacollection"
     region = var.global_values.aws_region
     # No assume_role needed as everything is deployed in the Payer account
   }
   ```

> **Note:** Single-account deployment in your Payer account is simpler for testing but lacks the security benefits and separation of concerns provided by the recommended cross-account architecture. For production environments, we strongly recommend the cross-account approach.

</details>

<details>
<summary><b>Is there an automated tool for single-account deployment?</b></summary>

Yes, we provide a testing framework in the `terraform-test` directory that simplifies single-account deployment for testing purposes. This framework includes scripts that automatically handle the necessary modifications to deploy everything in a single account.

For detailed instructions on using this testing framework, refer to the [README.md in the terraform-test directory](../terraform-test/README.md).

</details>

<details>
<summary><b>How do I deploy resources manually to specific accounts?</b></summary>

For users who need granular control over deployment or want to deploy resources manually to specific accounts, you can split the Terraform module into individual components:

### Manual Deployment Steps:

1. **Split the main.tf file** into separate files for each stack:
   ```
   data-exports-destination.tf  # For Data Collection account
   data-exports-source.tf       # For Payer account  
   dashboards.tf               # For Data Collection account
   ```

2. **Create separate variable files** for each component:
   ```
   variables-destination.tf
   variables-source.tf
   variables-dashboards.tf
   ```

3. **Split outputs.tf** into component-specific output files:
   ```
   outputs-destination.tf
   outputs-source.tf
   outputs-dashboards.tf
   ```

4. **Configure separate provider configurations** for each account:
   ```hcl
   # For Payer account deployment
   provider "aws" {
     region = "us-east-1"
     # Payer account credentials
   }
   
   # For Data Collection account deployment
   provider "aws" {
     region = "us-east-1"
     # Data Collection account credentials
   }
   ```

5. **Deploy in sequence**:
   ```bash
   # Step 1: Deploy Data Exports Destination (Data Collection account)
   terraform init
   terraform apply -target=aws_cloudformation_stack.cid_dataexports_destination
   
   # Step 2: Deploy Data Exports Source (Payer account)
   # Switch to Payer account credentials
   terraform apply -target=aws_cloudformation_stack.cid_dataexports_source
   
   # Step 3: Deploy Dashboards (Data Collection account)
   # Switch back to Data Collection account credentials
   terraform apply -target=aws_cloudformation_stack.cloud_intelligence_dashboards
   ```

### Important Considerations:

- **Dependencies**: Ensure proper dependency order (Destination → Source → Dashboards)
- **Cross-account references**: You'll need to manually manage cross-account resource references
- **State management**: Consider using separate state files for each account
- **Credentials**: Manually switch AWS credentials/profiles between deployments
- **Complexity**: This approach requires advanced Terraform knowledge and careful coordination

**Note**: Manual deployment is significantly more complex than the automated cross-account approach provided by this module. We recommend using the standard module unless you have specific requirements that necessitate manual control.

</details>

## Additional Resources

* [AWS Cloud Intelligence Dashboards Documentation](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/)
* [Foundational Dashboards Deployment Documentation](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboard-foundational.html)
* [QuickSight Documentation](https://docs.aws.amazon.com/quicksight/latest/user/welcome.html)