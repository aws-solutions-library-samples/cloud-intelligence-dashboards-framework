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

  * Default provider for the Payer account
  * Provider with "destination\_account" alias for the Data Collection account

## Quick Start

1. Configure your AWS credentials for both accounts
2. Create a `terraform.tfvars` file with your global values
3. Run the standard Terraform workflow:

```bash
terraform init
terraform plan
terraform apply
```

## Configuration

### Required Variables

Configure these values in your `terraform.tfvars` file:

```hcl
global_values = {
  destination_account_id = "123456789012"      # 12-digit Data Collection account ID
  source_account_ids     = "987654321098"      # Comma-separated list of Payer account IDs
  aws_region             = "us-east-1"         # AWS region for deployment
  quicksight_user        = "user/example"      # QuickSight username
  cid_cfn_version        = "4.2.7"             # CID CloudFormation version - Supporting from 4.2.7
  data_export_version    = "0.5.0"             # Data Export version
  environment            = "dev"               # Environment (dev, staging, prod)
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

### Optional Configuration

The module includes sensible defaults for all other parameters. You can override them in your `terraform.tfvars` file as needed:

```hcl
# Data Exports Destination Configuration (Data Collection account)
cid_dataexports_destination = {
  resource_prefix  = "cid"
  manage_cur2      = "yes"
  manage_focus     = "no"
  manage_coh       = "no"
  enable_scad      = "yes"
  role_path        = "/"
  time_granularity = "HOURLY"
}

# Data Exports Source Configuration (Payer account)
cid_dataexports_source = {
  source_resource_prefix  = "cid"
  source_manage_cur2      = "yes"
  source_manage_focus     = "no"
  source_manage_coh       = "no"
  source_enable_scad      = "yes"
  source_role_path        = "/"
  source_time_granularity = "HOURLY"
}

# Dashboard Configuration (Data Collection account)
cloud_intelligence_dashboards = {
  # Dashboard Selection
  deploy_cudos_v5                    = "yes"
  deploy_cost_intelligence_dashboard = "yes"
  deploy_kpi_dashboard               = "yes"
  deploy_tao_dashboard               = "no"
  deploy_compute_optimizer_dashboard = "no"

  # Other configurations available - see variables.tf for all options
}
```

## Important Technical Parameters (Do Not Modify)

The following parameters are managed internally by the CID deployment and **must not be changed**. Modifying these may lead to deployment failure or broken dashboards:

| Parameter                     | Purpose                                    |
| ----------------------------- | ------------------------------------------ |
| `athena_workgroup`            | Used by Athena to run queries              |
| `athena_query_results_bucket` | Stores Athena query results for QuickSight |
| `database_name`               | Athena/Glue database used by dashboards    |
| `cur_table_name`              | Name of the legacy CUR table if applicable |
| `suffix`                      | Unique stack identifier                    |
| `lambda_layer_bucket_prefix`  | Bucket prefix for Lambda layers            |
| `deployment_type`             | Deployment mechanism (Terraform/CFN)       |

These values are mapped to CloudFormation parameters grouped under **"Technical Parameters. Please do not change."** and are only overridden internally by Terraform.

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

| Dashboard         | Variable                             | Default |
| ----------------- | ------------------------------------ | ------- |
| CUDOS v5          | `deploy_cudos_v5`                    | yes     |
| Cost Intelligence | `deploy_cost_intelligence_dashboard` | yes     |
| KPI               | `deploy_kpi_dashboard`               | yes     |
| TAO               | `deploy_tao_dashboard`               | no      |
| Compute Optimizer | `deploy_compute_optimizer_dashboard` | no      |

## Outputs

After successful deployment, the module provides the following outputs:

| Output                                  | Description                    |
| --------------------------------------- | ------------------------------ |
| `cid_dataexports_destination_outputs`   | Outputs from destination stack |
| `cid_dataexports_source_outputs`        | Outputs from source stack      |
| `cloud_intelligence_dashboards_outputs` | QuickSight dashboard outputs   |

Access the dashboard URLs from the outputs to view your dashboards in QuickSight.

## Important Notes

* Stack creation can take up to 60 minutes
* All stacks require `CAPABILITY_IAM` and `CAPABILITY_NAMED_IAM` permissions
* The module uses S3 for Terraform state storage
* QuickSight Enterprise subscription is required in the Data Collection account
* Cross-account IAM roles are created automatically

## Customization

### Backend Configuration

The module uses an S3 backend for state storage. Configure your backend in a `backend.tf` file:

```hcl
terraform {
  backend "s3" {
    bucket       = "your-terraform-state-bucket"
    key          = "terraform/cid/terraform.tfstate"
    region       = "us-east-1"   # Replace with your desired region
    use_lockfile = true          # terraform-state-lock
    encrypt      = true
  }
}
```

### Provider Configuration

Configure the AWS providers for both accounts in a `provider.tf` file:

```hcl
provider "aws" {
  region = var.global_values.aws_region
  # Payer account credentials
}

provider "aws" {
  alias  = "destination_account"
  region = var.global_values.aws_region
  # Data Collection account credentials
  assume_role {
    role_arn = "arn:aws:iam::${var.global_values.destination_account_id}:role/YourCrossAccountRole"
  }
}
```

## Resource Details

### Data Exports Source Stack

* Deployed in the Payer account
* Creates IAM roles for cross-account access
* Sets up CUR report configuration
* Configures S3 bucket policies for data sharing

### Data Exports Destination Stack

* Deployed in the Data Collection account
* Creates S3 buckets for data collection
* Sets up Athena database and tables
* Configures IAM roles and policies
* Manages CUR, FOCUS, and COH data as configured

### Cloud Intelligence Dashboards Stack

* Deployed in the Data Collection account
* Deploys selected QuickSight dashboards
* Configures data sources and datasets
* Sets up necessary IAM permissions
* Creates dashboard sharing as configured

## Timeouts

All CloudFormation stacks are configured with 60-minute timeouts for create, update, and delete operations.

## FAQ

<details>
<summary><b>How do I backfill historical cost data?</b></summary>

To backfill historical data for the Data Export, follow the instructions in the [CID Documentation Backfill Data Export section](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html#backfill-data-export).

This process allows you to populate your dashboards with historical cost and usage data, ensuring you have a complete view of your AWS spending over time.

</details>

<details>
<summary><b>Can I deploy everything in a single account instead of using cross-account setup?</b></summary>

While the cross-account setup is recommended for production environments, you can deploy the entire solution in your Payer account without requiring a separate Data Collection account. This single-account approach is simpler for testing or development purposes. To do this:

1. **Modify main.tf**:
   * Comment out or remove the `resource "aws_cloudformation_stack" "cid_dataexports_source"` block
   * Update any dependencies that reference this resource

2. **Modify outputs.tf**:
   * Remove or comment out the `output "cid_dataexports_source_outputs"` block

3. **Remove the variable from terraform.tfvars**:
   * Remove or comment out the `cid_dataexports_source` variable block

4. **Update terraform.tfvars**:

   ```hcl
   global_values = {
     destination_account_id = "123456789012"      # Your Payer account ID
     source_account_ids     = "123456789012"      # Same Payer account ID
     aws_region             = "us-east-1"         # AWS region for deployment
     quicksight_user        = "user/example"      # QuickSight username
     cid_cfn_version        = "4.2.5"             # CID CloudFormation version
     data_export_version    = "0.5.0"             # Data Export version
     environment            = "dev"               # Environment (dev, staging, prod)
   }
   ```

5. **Simplify provider.tf**:

   ```hcl
   provider "aws" {
     region = var.global_values.aws_region
   }

   provider "aws" {
     alias  = "destination_account"
     region = var.global_values.aws_region
     # No assume_role needed as everything is deployed in the Payer account
   }
   ```

This configuration will deploy only the Data Exports Destination Stack and the Cloud Intelligence Dashboards Stack directly in your Payer account, skipping the separate Source Stack that would normally be deployed in a cross-account setup.

> **Note:** Single-account deployment in your Payer account is simpler for testing but lacks the security benefits and separation of concerns provided by the recommended cross-account architecture. For production environments, we strongly recommend the cross-account approach. For more details on the recommended architecture, see the [CID Architecture Documentation](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html#architecture).

</details>

<details>
<summary><b>Is there an automated tool for single-account deployment?</b></summary>

Yes, we provide a testing framework in the `terraform-test` directory that simplifies single-account deployment for testing purposes. This framework includes scripts that automatically handle the necessary modifications to deploy everything in a single account.

For detailed instructions on using this testing framework, refer to the [README.md in the terraform-test directory](../terraform-test/README.md). The testing framework:

1. Automatically comments out the source stack resource
2. Configures the proper account IDs
3. Sets up the appropriate provider configuration
4. Provides options for local or S3 backend configuration

This is the recommended approach for testing and development environments when you want to use a single account.

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
