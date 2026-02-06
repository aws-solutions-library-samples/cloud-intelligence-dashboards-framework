# Cloud Intelligence Dashboards (CUDOS Framework)

[![PyPI version](https://badge.fury.io/py/cid-cmd.svg)](https://badge.fury.io/py/cid-cmd)


## Table of Contents
1. [Overview](#overview)
1. [Architecture of Foundational Dashboards](#architecture-of-foundational-dashboards)
1. [Architecture of Advanced Dashboards](#architecture-of-advanced-dashboards)
1. [Prerequisites](#prerequisites)
1. [Deployment Steps](#deployment-steps)
1. [Terraform Configuration](#terraform-configuration)
   - [Quick Start for Users](#quick-start-for-users)
   - [Dashboard Types](#dashboard-types)
   - [File Structure](#file-structure)
   - [Backend and Provider Configuration](#backend-and-provider-configuration)
   - [Standalone Dashboard Deployments](#standalone-dashboard-deployments)
   - [Advanced Configuration](#advanced-configuration)
1. [Cleanup](#cleanup)
1. [FAQ](#faq)
1. [Changelogs](#changelogs)
1. [Feedback](#feedback)
1. [Security](#security)
1. [License](#license)
1. [Notices](#notices)

## Overview
The Cloud Intelligence Dashboards is an open-source framework, lovingly cultivated and maintained by a group of customer-obsessed AWSers, that gives customers the power to get high-level and granular insight into their cost and usage data. Supported by the Well-Architected framework, the dashboards can be deployed by any customer using a CloudFormation template or a command-line tool in their environment in under 30 minutes. These dashboards help you to drive financial accountability, optimize cost, track usage goals, implement best practices for governance, and achieve operational excellence across all your organization.

Cloud Intelligence Dashboards Framework provides AWS customers with [more then 20 Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html).
* Foundational Dashboards - A set of main Dashboards that only require Cost and Usage Report(CUR)
* Advanced Dashboards - Require CID Data Collection and CUR
* Additional Dashboards - Require various custom datasources or created for very specific use cases
* Additional CUR-based Dashboards - Require only CUR data and a foundational dashboard deployment

We recommend starting with deployment of [Foundational Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboard-foundational.html). Then deploy [Data Collection](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/data-collection.html) and [Advanced Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html#advanced-dashboards). Check for [Additional](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html#additional) Dashboards and Additional CUR-based Dashboards.


[![Documentation >](assets/images/documentation.svg)](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html)


## Architecture of Foundational Dashboards

![Foundational Architecture](assets/images/foundational-architecture.png  "Foundational Architecture")
1. [AWS Data Exports](https://aws.amazon.com/aws-cost-management/aws-data-exports/) delivers daily the Cost & Usage Report (CUR2) to an [Amazon S3 Bucket](https://aws.amazon.com/s3/) in the Management Account.
2. [Amazon S3](https://aws.amazon.com/s3/) replication rule copies Export data to a dedicated Data Collection Account S3 bucket automatically.
3. [Amazon Athena](https://aws.amazon.com/athena/) allows querying data directly from the S3 bucket using an [AWS Glue](https://aws.amazon.com/glue/) table schema definition.
4. [Amazon QuickSight](https://aws.amazon.com/quicksight/) creates datasets from [Amazon Athena](https://aws.amazon.com/athena/), refreshes daily and caches in [SPICE](https://docs.aws.amazon.com/quicksight/latest/user/spice.html)(Super-fast, Parallel, In-memory Calculation Engine) for [Amazon QuickSight](https://aws.amazon.com/quicksight/)
5. User Teams (Executives, FinOps, Engineers) can access Cloud Intelligence Dashboards in [Amazon QuickSight](https://aws.amazon.com/quicksight/). Access is secured through [AWS IAM](https://aws.amazon.com/iam/), IIC ([AWS IAM Identity Center](https://aws.amazon.com/iam/identity-center/), formerly SSO), and optional [Row Level Security](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/row-level-security.html).

This foundational architecture is recommended for starting and allows deployment of [Foundational Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboard-foundational.html) like CUDOS, CID and KPI.

## Architecture of Advanced Dashboards
![Advanced Architecture](assets/images/advanced-architecture.png  "Foundational Architecture")

1. [AWS Data Exports](https://aws.amazon.com/aws-cost-management/aws-data-exports/) delivers daily the Cost & Usage Report (CUR2) to an [Amazon S3 Bucket](https://aws.amazon.com/s3/) in the Management Account.
2. [Amazon S3](https://aws.amazon.com/s3/) replication rule copies Export data to a dedicated Data Collection Account S3 bucket automatically.
3. [Amazon Athena](https://aws.amazon.com/athena/) allows querying data directly from the S3 bucket using an [AWS Glue](https://aws.amazon.com/glue/) table schema definition.
4. [Amazon QuickSight](https://aws.amazon.com/quicksight/) creates datasets from [Amazon Athena](https://aws.amazon.com/athena/), refreshes daily and caches in [SPICE](https://docs.aws.amazon.com/quicksight/latest/user/spice.html)(Super-fast, Parallel, In-memory Calculation Engine) for [Amazon QuickSight](https://aws.amazon.com/quicksight/)
5. User Teams (Executives, FinOps, Engineers) can access Cloud Intelligence Dashboards in [Amazon QuickSight](https://aws.amazon.com/quicksight/). Access is secured through [AWS IAM](https://aws.amazon.com/iam/), IIC ([AWS IAM Identity Center](https://aws.amazon.com/iam/identity-center/), formerly SSO), and optional [Row Level Security](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/row-level-security.html).
6. Optionally, the Advanced data collection can be deployed to enable advanced dashboards based on [AWS Trusted Advisor](https://aws.amazon.com/premiumsupport/trustedadvisor/), [AWS Health](https://aws.amazon.com/premiumsupport/technology/aws-health-dashboard/) Events and other sources. Additional data is retrieved from [AWS Organizations](https://aws.amazon.com/organizations/) or Linked Accounts. In this case [Amazon EventBridge](https://aws.amazon.com/eventbridge/) rule triggers an [AWS Step Functions](https://aws.amazon.com/step-functions/) workflow for data collection modules on a configurable schedule.
7. The "Account Collector" [AWS Lambda](https://aws.amazon.com/lambda/) in AWS Step Functions retrieves linked account details using AWS Organizations API.
8. The "Data Collection" Lambda function in AWS Step Functions assumes a role in each linked account to retrieve account-specific data via [AWS SDK](https://aws.amazon.com/developer/tools/).
9. Retrieved data is stored in a centralized Amazon S3 Bucket.
10. [Advanced Cloud Intelligence Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html#advanced-dashboards) leverage Amazon Athena and Amazon QuickSight for comprehensive data analysis.

This advanced data collection architecture allows deployment of [Advanced Dashboards](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboards.html#advanced-dashboards) like Trusted Advisor Dashboard, Health, Graviton, Compute Optimizer Dashboard and many more.


**Additional Notes:**
-
 Free trial available for 30 days for 4 QuickSight users
- Actual costs may vary based on specific usage and data volume

Please use the AWS Pricing Calculator for precise estimation.

## Prerequisites
You need access to AWS Accounts. We recommend deployment of the Dashboards in a dedicated Data Collection Account, other than your Management (Payer) Account. We provide CloudFormation templates to copy CUR 2.0 data from your Management Account to a dedicated one. You can use it to aggregate data from multiple Management (Payer) Accounts or multiple Linked Accounts.

If you do not have access to the Management/Payer Account, you can still collect the data across multiple Linked accounts using the same approach.

The ownership of CID is usually with the FinOps team, who do not have administrative access. However, they require specific privileges to install and operate CID dashboards. To assist the Admin team in granting the necessary privileges to the CID owners, a CFN template is provided. This template, located at [CFN template](cfn-templates/cid-admin-policies.yaml), takes an IAM role name as a parameter and adds the required policies to the role.


## Deployment Steps
There are several ways we can deploy dashboards:
1. Using cid-cmd tool from the command line
1. [CloudFormation Template](./cfn-templates/cid-cfn.yml) using cid-cmd tool in Amazon Lambda. (Recommended)
1. [Terraform Configuration](#Terraform-Configuration) for infrastructure as code deployment

Please refer to the deployment documentation [here](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html).

[![Deployment Guide >](assets/images/deployment-guide-button.svg)](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/deployment-in-global-regions.html)

## Terraform Configuration

### Quick Start for Users

#### 1. Configure Your Deployment

**Edit `user-config.tf`** - This is the main file you need to modify:

```hcl
# Update these values for your environment
global_values = {
  destination_account_id = "123456789012"     # Your AWS Account ID
  source_account_ids     = "123456789012"     # Same or different account IDs
  aws_region            = "us-east-1"         # Your preferred region
  quicksight_user       = "user@example.com" # Your QuickSight username
  cid_cfn_version       = "4.3.6"           # CID version to deploy
  data_export_version   = "0.5.0"           # Data export version
  environment           = "prod"              # dev, staging, or prod
}

# Choose which dashboards to deploy
dashboards = {
  # Foundational (at least one required)
  cudos_v5          = "yes"  # CUDOS v5 Dashboard
  cost_intelligence = "no"   # Cost Intelligence Dashboard  
  kpi               = "no"   # KPI Dashboard
  
  # Additional CUR-based Dashboards
  trends       = "yes"  # Trends Dashboard
  datatransfer = "no"   # Data Transfer Cost Analysis
  marketplace  = "no"   # AWS Marketplace Dashboard
  connect      = "no"   # Amazon Connect Cost Insight
  containers   = "no"   # SCAD Containers Cost Allocation
}

# For standalone dashboard deployments (advanced use case)
# allow_standalone_dashboard = true  # Uncomment if deploying without foundational dashboards
```

#### 2. Deploy

```bash
terraform init
terraform plan
terraform apply
```

### Dashboard Types

#### Foundational Dashboards
At least one foundational dashboard is required as they generate the CUR data needed by additional dashboards.

| Dashboard | Description |
|-----------|-------------|
| `cudos_v5` | CUDOS v5 Dashboard - Comprehensive cost analysis |
| `cost_intelligence` | Cost Intelligence Dashboard - Advanced cost insights |
| `kpi` | KPI Dashboard - Key performance indicators |

#### Additional CUR-based Dashboards
These require a foundational dashboard to be deployed first and use the same CUR data.

| Dashboard | Description | Dataset |
|-----------|-------------|---------|
| `trends` | Trends Dashboard | daily-anomaly-detection |
| `datatransfer` | Data Transfer Cost Analysis | data_transfer_view |
| `marketplace` | AWS Marketplace Dashboard | marketplace_view |
| `connect` | Amazon Connect Cost Insight | resource_connect_view |
| `containers` | SCAD Containers Cost Allocation | scad_cca_summary_view |

### File Structure

- **`user-config.tf`** - 📝 **EDIT THIS** - Main configuration file for users
- **`variables.tf`** - ⚙️ **Advanced users only** - Technical/internal variables
- **`dashboards.tf`** - 🏗️ **Don't edit** - Resource definitions
- **`locals.tf`** - 🏗️ **Don't edit** - Internal logic and configurations
- **`outputs.tf`** - 📊 **Don't edit** - Output definitions
- **`backend.tf`** - 💾 **CREATE THIS** - Backend configuration (see instructions below)
- **`providers.tf`** - 🔧 **CREATE THIS** - Provider configurations (see instructions below)

### Backend and Provider Configuration

The module requires backend and provider configuration files that are not included in the repository. You need to create these files before deployment:

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
}

provider "aws" {
  alias  = "datacollection"
  region = var.global_values.aws_region
}
```

### Standalone Dashboard Deployments

For advanced use cases where you want to deploy individual dashboards without foundational dashboards (e.g., when foundational dashboards are already deployed elsewhere):

```hcl
# Enable standalone deployment
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

### Advanced Configuration

If you need to modify technical parameters, you can edit `variables.tf`, but this is typically not needed for standard deployments.

## Cleanup
Please refer to the documentation [here](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/dashboard-teardown.html).

## FAQ
Please refer to the documentation [here](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/faq.html).

## Changelogs
For dashboards please check change log [here](changes/)
For CID deployment tool, including CLI and CFN please check [Releases](/releases)

## Feedback
Please reference to [this page](https://docs.aws.amazon.com/guidance/latest/cloud-intelligence-dashboards/feedback-support.html)

## Security
When you build systems on AWS infrastructure, security responsibilities are shared between you and AWS. This [shared responsibility
model](https://aws.amazon.com/compliance/shared-responsibility-model/) reduces your operational burden because AWS operates, manages, and
controls the components including the host operating system, the virtualization layer, and the physical security of the facilities in
which the services operate. For more information about AWS security, visit [AWS Cloud Security](http://aws.amazon.com/security/).

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE) file.

## Notices
Dashboards and their content: (a) are for informational purposes only, (b) represent current AWS product offerings and practices, which are subject to change without notice, and (c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS content, products or services are provided "as is" without warranties, representations, or conditions of any kind, whether express or implied. The responsibilities and liabilities of AWS to its customers are controlled by AWS agreements, and this document is not part of, nor does it modify, any agreement between AWS and its customers.