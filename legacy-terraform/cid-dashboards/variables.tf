variable "stack_name" {
  type        = string
  description = "CloudFormation stack name for Cloud Intelligence Dashboards deployment"
}

variable "template_bucket" {
  type        = string
  description = "S3 bucket where the Cloudformation template will be uploaded. Must already exist and be in the same region as the stack."
}

variable "template_key" {
  type        = string
  description = "Name of the S3 path/key where the Cloudformation template will be created. Defaults to cid-cfn.yml"
  default     = "cid-cfn.yml"
}

variable "stack_parameters" {
  type        = map(string)
  description = <<-EOF
    CloudFormation stack parameters. For the full list of available parameters, refer to
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
      - PermissionsBoundary: Leave blank if you don't need to set a boundary for roles
      - RolePath: Path for roles where PermissionBoundaries can limit location
  EOF
}

variable "stack_tags" {
  type        = map(string)
  description = "Tag key-value pairs to apply to the stack"
  default     = null
}

variable "stack_policy_body" {
  type        = string
  description = "String containing the stack policy body. Conflicts with stack_policy_url."
  default     = null
}

variable "stack_policy_url" {
  type        = string
  description = "Location of a file containing the stack policy body. Conflicts with stack_policy_body."
  default     = null
}

variable "stack_notification_arns" {
  type        = list(string)
  description = "A list of SNS topic ARNs to publish stack related events."
  default     = []
}

variable "stack_iam_role" {
  type        = string
  description = "The ARN of an IAM role that AWS CloudFormation assumes to create the stack (default behavior is to use the previous role if available, or current user permissions otherwise)."
  default     = null
}
