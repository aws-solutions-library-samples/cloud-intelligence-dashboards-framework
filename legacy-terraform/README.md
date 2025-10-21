# DEPRECATED - PLEASE DO NOT USE TERRAFORM MODULES FOR ANY NEW DEPLOYMENT
We are currently working on a new generation of terraform wrappers. Please stay tunned.

# CID Terraform Modules

We provide Terraform modules for your convenience in deploying CID dashboards.

## Module List

Currently, the following modules are provided:

### [cur-setup-destination](./cur-setup-destination/README.md) (Data Collection Account CUR Setup)

Terraform module to set up a Cost and Usage Report in the data collection
account for use in Cost Intelligence Dashboards. The module creates an S3 bucket
with the necessary permissions for replicating CUR data from one or more payer
accounts. If the data collection account is part of a different payer and is not
covered in the CUR from the source accounts, the module can create a new Cost
and Usage Report local to the data collection account.

Review [module documentation](./cur-setup-destination/README.md) for details
on module requirements, inputs, and outputs.

### [cur-setup-source](./cur-setup-source/README.md) (Payer Account CUR Setup)

Terraform module to set up a Cost and Usage Report in a payer account
for use in Cost Intelligence Dashboards. The module creates an S3 bucket with
the necessary permissions and configuration to replicate CUR data to the
data collection account. If you are deploying Cost Intelligence Dashboards
for a multi-payer environment, you can deploy one instance of this module for
each payer account.

Review [module documentation](./cur-setup-source/README.md) for details
on module requirements, inputs, and outputs.

### [cid-dashboards](./cid-dashboards/README.md) (Dashboard Deployment)

Terraform module to deploy CID dashboards. This module is a wrapper around
CloudFormation to allow you to deploy CID dashboards using your existing
Terraform workflows. Under the hood, the module will deploy a CloudFormation
stack which will provision the necessary resources and a custom Lambda
function to create the dashboards using `cid-cmd`.

Review [module documentation](./cid-dashboards/README.md) for details
on module requirements, inputs, and outputs.

## Module Usage

The modules provided are intended to be used as remote modules using
the GitHub / git source as below.

```hcl
module "example" {
  source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/<module name>"
  ...
}
```

You can customize the modules using input variables and retrieve output values
from the module outputs.

### Version Locking

For production deployments, you should lock module versions to a release
tag to better control when and what updates are made. To specify the
release tag to use, append `?ref=VERSION` to the module source. For
example, the following source reference will use the Terraform module
and Cloudformation template from version 0.2.14 of this module:

```
source = "github.com/aws-samples/aws-cudos-framework-deployment//terraform-modules/cur-setup-source?ref=0.2.14"
```

For a complete list of release tags, visit https://github.com/aws-samples/aws-cudos-framework-deployment/tags.

*Note: The same syntax can be used to use pre-release/beta versions by
specifying a branch name instead of a tag name*

## Deployment Instructions

### Step 1: CUR Setup

If you have not set up CUR for use with CID dashboards, first deploy the
[cur-setup-destination](./cur-setup-destination/README.md) module to your
**data collection account.**

After deploying the module to your data collection account, deploy the
[cur-setup-source](./cur-setup-source/README.md) module to your **payer account(s).**

*Note: You can choose to deploy both modules in a single Terraform root module/workspace.
If you do this, you must properly pass the S3 Bucket ARN from the `cur-setup-destination` module
so that Terraform can infer the dependency correctly.*

#### Example Usage

```hcl
# cur_setup.tf

provider "aws" {
  profile = "payer"
  region  = "us-west-2"
  alias   = "payer"
}

provider "aws" {
  profile = "payer"
  region  = "us-east-1"
  alias   = "payer_useast1"
}

provider "aws" {
  profile = "data_collection"
  region  = "us-west-2"
  alias   = "data_collection"
}

provider "aws" {
  profile = "data_collection"
  region  = "us-east-1"
  alias   = "data_collection_useast1"
}

# Configure exactly one destination account
module "cur_data_collection_account" {
  source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/cur-setup-destination"

  source_account_ids = ["1234567890"]
  create_cur         = false # Set to true to create an additional CUR in the aggregation account

  # Provider alias for us-east-1 must be passed explicitly (required for CUR setup)
  # Optionally, you may pass the default aws provider explicitly as well
  providers = {
    aws         = aws.data_collection
    aws.useast1 = aws.data_collection_useast1
  }
}

# Configure one or more source (payer) accounts
module "cur_payer" {
  source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/cur-setup-source"

  destination_bucket_arn = module.cur_data_collection_account.cur_bucket_arn

  # Provider alias for us-east-1 must be passed explicitly (required for CUR setup)
  # Optionally, you may pass the default aws provider explicitly as well
  providers = {
    aws         = aws.payer
    aws.useast1 = aws.payer_useast1
  }
}
```

### Step 2: Dashboard Prerequisite Setup

After enabling CUR with Terraform as describe above, a few steps are necessary
to prepare for dashboard deployment:
  1. Complete prerequisites for each dashboard you are deploying and the Quicksight setup as described in [Before You Start](../README.md#before-you-start). There is no need to complete the Athena workgroup setup unless you are using a custom workgroup. A workgroup and query destination setup will be created for you automatically.
  2. Create an S3 bucket to upload the CloudFormation template

### Step 3: Dashboard Setup

In a separate Terraform root module/workspace, deploy the [cid-dashboards](./cur-setup-source/README.md) module to your data collection (destination) account.
Only deploy one instance of this module. To deploy multiple dashboards, customize
the `stack_parameters` to deploy the desired dashboards. Refer to the
[module documentation](./cur-setup-source/README.md) for the most frequently
used `stack_parameters`.


#### Example Usage

```hcl
# dashboard_deployment.tf

module "cid_dashboards" {
    source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/cid-dashboards"

    stack_name      = "Cloud-Intelligence-Dashboards"
    template_bucket = "UPDATEME" # Update with S3 bucket name
    stack_parameters = {
      "PrerequisitesQuickSight"            = "yes"
      "PrerequisitesQuickSightPermissions" = "yes"
      "QuickSightUser"                     = "UPDATEME"
      "DeployCUDOSDashboard"               = "yes"
      "DeployCostIntelligenceDashboard"    = "yes"
      "DeployKPIDashboard"                 = "yes"
    }
}
```

