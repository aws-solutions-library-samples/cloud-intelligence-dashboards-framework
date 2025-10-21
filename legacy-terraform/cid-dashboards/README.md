# CID Terraform Module: cid-dashboards

Terraform module to deploy CID dashboards. This module is a wrapper around CloudFormation
to allow you to deploy CID dashboards using your existing Terraform workflows. Under the
hood, the module will deploy a CloudFormation stack which will provision the necessary
resources and a custom Lambda function to create the dashboards using `cid-cmd`.

## Before You Start

  - Existing S3 bucket to upload the CloudFormation template
  - Complete prerequisites in [Before You Start](../../README.md#before-you-start) including CUR and Quicksight setup

## Example Usage

```hcl
module "cid_dashboards" {
    source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/cid-dashboards"

    stack_name      = "Cloud-Intelligence-Dashboards"
    template_bucket = "UPDATEME"
  
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

## Version Locking

For production deployments, you should lock the version of this module to a release tag to better
control when and what updates are made. To specify the release tag to use, append `?ref=VERSION`
to the module source. For example, the following source reference will use the Terraform module
and Cloudformation template from version 0.2.13 of this module:

```
source = "github.com/aws-samples/aws-cudos-framework-deployment//terraform-modules/cid-dashboards?ref=0.2.13"
```

For a complete list of release tags, visit https://github.com/aws-samples/aws-cudos-framework-deployment/tags.

## Troubleshooting

Because this module is primarily a wrapper for CloudFormation, Terraform output may not be sufficient
for debugging if deployment fails. For additional troubleshooting information, refer to the CloudFormation
console for details on stack operation, resources, and error output. Additionally, you can refer to logs
for the custom resource Lambda function "CidCustomDashboardResource"if dashboards fail to deploy.

<!-- BEGIN_TF_DOCS -->
## Requirements

The following requirements are needed by this module:

- terraform (>= 1.0)

- aws (>= 3.0)

## Resources

The following resources are used by this module:

- [aws_cloudformation_stack.cid](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudformation_stack) (resource)
- [aws_s3_object.template](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_object) (resource)
- [aws_s3_bucket.template_bucket](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/s3_bucket) (data source)

## Required Inputs

The following input variables are required:

### stack\_name

Description: CloudFormation stack name for Cloud Intelligence Dashboards deployment

Type: `string`

### stack\_parameters

Description: CloudFormation stack parameters. For the full list of available parameters, refer to  
https://github.com/aws-samples/aws-cudos-framework-deployment/blob/main/cfn-templates/cid-cfn.yml.  
For most setups, you will want to set the following parameters:
  - PrerequisitesQuickSight: yes/no
  - PrerequisitesQuickSightPermissions: yes/no
  - QuickSightUser: Existing quicksight user
  - QuickSightDataSetRefreshSchedule: Cron expression to refresh spice datasets daily outside of business hours. Default is 4 AM UTC, which should work for most customers in US and EU time zones
  - CURBucketPath: Leave as default is if CUR was created with CloudFormation (cur-aggregation.yaml). If it was a manually created CUR, the path entered below must be for the directory that contains the years partition (s3://curbucketname/prefix/curname/curname/).
  - OptimizationDataCollectionBucketPath: The S3 path to the bucket created by the Cost Optimization Data Collection Lab. The path will need point to a folder containing /optics-data-collector folder. Required for TAO and Compute Optimizer dashboards.
  - DataBuketsKmsKeyArns: Comma-delimited list of KMS key ARNs ("*" is also valid). Include any KMS keys used to encrypt your CUR or Cost Optimization Data S3 data
  - DeployCUDOSDashboard: (yes/no, default no)
  - DeployCostIntelligenceDashboard: (yes/no, default no)
  - DeployKPIDashboard: (yes/no, default no)
  - DeployTAODashboard: (yes/no, default no)
  - DeployComputeOptimizerDashboard: (yes/no, default no)

Type: `map(string)`

### template\_bucket

Description: S3 bucket where the Cloudformation template will be uploaded. Must already exist and be in the same region as the stack.

Type: `string`

## Optional Inputs

The following input variables are optional (have default values):

### stack\_iam\_role

Description: The ARN of an IAM role that AWS CloudFormation assumes to create the stack (default behavior is to use the previous role if available, or current user permissions otherwise).

Type: `string`

Default: `null`

### stack\_notification\_arns

Description: A list of SNS topic ARNs to publish stack related events.

Type: `list(string)`

Default: `[]`

### stack\_policy\_body

Description: String containing the stack policy body. Conflicts with stack\_policy\_url.

Type: `string`

Default: `null`

### stack\_policy\_url

Description: Location of a file containing the stack policy body. Conflicts with stack\_policy\_body.

Type: `string`

Default: `null`

### stack\_tags

Description: Tag key-value pairs to apply to the stack

Type: `map(string)`

Default: `null`

### template\_key

Description: Name of the S3 path/key where the Cloudformation template will be created. Defaults to cid-cfn.yml

Type: `string`

Default: `"cid-cfn.yml"`

## Outputs

The following outputs are exported:

### stack\_outputs

Description: CloudFormation stack outputs (map of strings)
<!-- END_TF_DOCS -->