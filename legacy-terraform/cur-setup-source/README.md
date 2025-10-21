# CID Terraform Module: cur-setup-source

Terraform module to set up a Cost and Usage Report in a source (payer) account
for use in Cost Intelligence Dashboards. The module creates an S3 bucket with the necessary
permissions and configuration to replicate CUR data to the destination/aggregation account.
If you are deploying Cost Intelligence Cashboards for a multi-payer environment, you can
one instance of this module for each payer account.

## Example Usage

> [!Note]
> For complete usage documentation of using this module together with the cur-setup-destination
module, refer to the main Terraform [Deployment Instructions](../README.md#deployment-instructions).

```hcl
provider "aws" {
  region = "us-west-2"
}

provider "aws" {
  region = "us-east-1"
  alias  = "useast1"
}

# Configure one or more source (payer) accounts
module "cur_source" {
  source = "github.com/aws-samples/aws-cudos-framework-deployment//legacy-terraform/cur-setup-source"

  destination_bucket_arn = "UPDATEME"

  # Provider alias for us-east-1 must be passed explicitly (required for CUR setup)
  providers = {
    aws.useast1 = aws.useast1
  }
}
```

## Version Locking

For production deployments, you should lock the version of this module to a release tag to better
control when and what updates are made. To specify the release tag to use, append `?ref=VERSION`
to the module source. For example, the following source reference will use the Terraform module
and Cloudformation template from version 0.2.13 of this module:

```
source = "github.com/aws-samples/aws-cudos-framework-deployment//terraform-modules/cur-setup-source?ref=0.2.13"
```

For a complete list of release tags, visit https://github.com/aws-samples/aws-cudos-framework-deployment/tags.

*Note: The same syntax can be used to use pre-release/beta versions by specifying a branch name
instead of a tag name*

<!-- BEGIN_TF_DOCS -->
## Requirements

The following requirements are needed by this module:

- terraform (>= 1.0)

- aws (>= 3.0)

## Providers

The following providers are used by this module:

- aws (>= 3.0)

- aws.useast1 (>= 3.0)

## Resources

The following resources are used by this module:

- [aws_cur_report_definition.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cur_report_definition) (resource)
- [aws_iam_role.replication](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) (resource)
- [aws_s3_bucket.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket) (resource)
- [aws_s3_bucket_lifecycle_configuration.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_lifecycle_configuration) (resource)
- [aws_s3_bucket_logging.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_logging) (resource)
- [aws_s3_bucket_ownership_controls.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_ownership_controls) (resource)
- [aws_s3_bucket_policy.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_policy) (resource)
- [aws_s3_bucket_public_access_block.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_public_access_block) (resource)
- [aws_s3_bucket_replication_configuration.replication](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_replication_configuration) (resource)
- [aws_s3_bucket_server_side_encryption_configuration.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_server_side_encryption_configuration) (resource)
- [aws_s3_bucket_versioning.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_versioning) (resource)
- [aws_caller_identity.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) (data source)
- [aws_iam_policy_document.bucket_policy](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) (data source)
- [aws_iam_policy_document.replication](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) (data source)
- [aws_iam_policy_document.s3_assume_role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) (data source)
- [aws_partition.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/partition) (data source)
- [aws_region.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) (data source)

## Required Inputs

The following input variables are required:

### destination\_bucket\_arn

Description: Destination Bucket ARN

Type: `string`

## Optional Inputs

The following input variables are optional (have default values):

### cur\_name\_suffix

Description: Suffix used to name the CUR report

Type: `string`

Default: `"cur"`

### enable\_split\_cost\_allocation\_data

Description: Enable split cost allocation data for ECS and EKS for this CUR report

Type: `bool`

Default: `false`

### kms\_key\_id

Description: !!!WARNING!!! EXPERIMENTAL - Do not use unless you know what you are doing. The correct key policies and IAM permissions  
on the S3 replication role must be configured external to this module.
  - The "billingreports.amazonaws.com" service must have access to encrypt objects with the key ID provided
  - See https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-config-for-kms-objects.html for information  
    on permissions required for replicating KMS-encrypted objects

Type: `string`

Default: `null`

### resource\_prefix

Description: Prefix used for all named resources, including the S3 Bucket

Type: `string`

Default: `"cid"`

### s3\_access\_logging

Description: S3 Access Logging configuration for the CUR bucket

Type:

```hcl
object({
    enabled = bool
    bucket  = string
    prefix  = string
  })
```

Default:

```json
{
  "bucket": null,
  "enabled": false,
  "prefix": null
}
```

### tags

Description: Map of tags to apply to module resources

Type: `map(string)`

Default: `{}`

## Outputs

The following outputs are exported:

### cur\_bucket\_arn

Description: ARN of the S3 Bucket where the Cost and Usage Report is delivered

### cur\_bucket\_name

Description: Name of the S3 Bucket where the Cost and Usage Report is delivered

### cur\_report\_arn

Description: ARN of the Cost and Usage Report

### replication\_role\_arn

Description: ARN of the IAM role created for S3 replication

### replication\_role\_name

Description: ARN of the IAM role created for S3 replication
<!-- END_TF_DOCS -->
