variable "cid_dataexports_destination" {
  type = object({
    resource_prefix  = optional(string, "cid")    # Prefix used for all named resources
    manage_cur2      = optional(string, "yes")    # Enable CUR 2.0 management
    manage_focus     = optional(string, "no")     # Enable FOCUS management
    manage_coh       = optional(string, "no")     # Enable Cost Optimization Hub management
    enable_scad      = optional(string, "yes")    # Enable Split Cost Allocation Data
    role_path        = optional(string, "/")      # Path for IAM roles
    time_granularity = optional(string, "HOURLY") # Time granularity for CUR 2.0
  })

  description = "Configuration for data exports child account settings"

  default = {}

  validation {
    condition     = can(regex("^[a-z0-9]+[a-z0-9-]{1,61}[a-z0-9]+$", var.cid_dataexports_destination.resource_prefix))
    error_message = "ResourcePrefix must match pattern ^[a-z0-9]+[a-z0-9-]{1,61}[a-z0-9]+$"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_destination.manage_cur2)
    error_message = "ManageCUR2 must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_destination.manage_focus)
    error_message = "ManageFOCUS must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_destination.manage_coh)
    error_message = "ManageCOH must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_destination.enable_scad)
    error_message = "EnableSCAD must be yes or no"
  }

  validation {
    condition     = contains(["HOURLY", "DAILY", "MONTHLY"], var.cid_dataexports_destination.time_granularity)
    error_message = "TimeGranularity must be HOURLY, DAILY, or MONTHLY"
  }
}

variable "cid_dataexports_source" {
  type = object({
    source_resource_prefix  = optional(string, "cid")    # Prefix used for all named resources in management account
    source_manage_cur2      = optional(string, "yes")    # Enable CUR 2.0 management in management account
    source_manage_focus     = optional(string, "no")     # Enable FOCUS management in management account
    source_manage_coh       = optional(string, "no")     # Enable Cost Optimization Hub management in management account
    source_enable_scad      = optional(string, "yes")    # Enable Split Cost Allocation Data in management account
    source_role_path        = optional(string, "/")      # Path for IAM roles in management account
    source_time_granularity = optional(string, "HOURLY") # Time granularity for CUR 2.0 in management account
  })

  description = "Configuration for data exports management account settings"

  default = {}

  validation {
    condition     = can(regex("^[a-z0-9]+[a-z0-9-]{1,61}[a-z0-9]+$", var.cid_dataexports_source.source_resource_prefix))
    error_message = "ResourcePrefix must match pattern ^[a-z0-9]+[a-z0-9-]{1,61}[a-z0-9]+$"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_source.source_manage_cur2)
    error_message = "ManageCUR2 must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_source.source_manage_focus)
    error_message = "ManageFOCUS must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_source.source_manage_coh)
    error_message = "ManageCOH must be yes or no"
  }

  validation {
    condition     = contains(["yes", "no"], var.cid_dataexports_source.source_enable_scad)
    error_message = "EnableSCAD must be yes or no"
  }

  validation {
    condition     = contains(["HOURLY", "DAILY", "MONTHLY"], var.cid_dataexports_source.source_time_granularity)
    error_message = "TimeGranularity must be HOURLY, DAILY, or MONTHLY"
  }
}

variable "cloud_intelligence_dashboards" {
  type = object({
    # Prerequisites Variables
    prerequisites_quicksight             = optional(string, "yes")
    prerequisites_quicksight_permissions = optional(string, "yes")
    lake_formation_enabled               = optional(string, "no")

    # CUR Parameters
    cur_version                        = optional(string, "2.0")
    deploy_cudos_v5                    = optional(string, "yes")
    deploy_cost_intelligence_dashboard = optional(string, "yes")
    deploy_kpi_dashboard               = optional(string, "yes")

    # Optimization Parameters
    optimization_data_collection_bucket_path = optional(string, "s3://cid-data-{account_id}")
    deploy_tao_dashboard                     = optional(string, "no")
    deploy_compute_optimizer_dashboard       = optional(string, "no")
    primary_tag_name                         = optional(string, "owner")
    secondary_tag_name                       = optional(string, "environment")

    # Technical Parameters
    athena_workgroup                     = optional(string, "")
    athena_query_results_bucket          = optional(string, "")
    database_name                        = optional(string, "")
    glue_data_catalog                    = optional(string, "AwsDataCatalog")
    suffix                               = optional(string, "")
    quicksight_data_source_role_name     = optional(string, "CidQuickSightDataSourceRole")
    quicksight_data_set_refresh_schedule = optional(string, "")
    lambda_layer_bucket_prefix           = optional(string, "aws-managed-cost-intelligence-dashboards")
    deploy_cudos_dashboard               = optional(string, "no")
    data_buckets_kms_keys_arns           = optional(string, "")
    deployment_type                      = optional(string, "Terraform")
    share_dashboard                      = optional(string, "yes")

    # Legacy Parameters
    keep_legacy_cur_table = optional(string, "no")
    cur_bucket_path       = optional(string, "s3://cid-{account_id}-shared/cur/")
    cur_table_name        = optional(string, "")
    permissions_boundary  = optional(string, "")
    role_path             = optional(string, "/")
  })

  default = {}
}

variable "global_values" {
  type = object({
    destination_account_id = optional(string)     # AWS Account Id where DataExport will be replicated to
    source_account_ids     = optional(string, "") # Comma separated list of source account IDs
    aws_region             = optional(string, "") # AWS region where the dashboard will be deployed
    quicksight_user        = optional(string)     # Quicksight user to share the dashboard with
    cid_cfn_version        = optional(string, "") # CloudFormation template using for the deployment, see description to get the semantic version number (e.g. 4.1.5) https://github.com/aws-solutions-library-samples/cloud-intelligence-dashboards-framework/blob/main/cfn-templates/cid-cfn.yml
    data_export_version    = optional(string, "") # CloudFormation template using for the deployment, see description to get the semantic version number (e.g. 0.5.0) https://github.com/aws-solutions-library-samples/cloud-intelligence-dashboards-data-collection/blob/main/data-exports/deploy/data-exports-aggregation.yaml
    environment            = optional(string, "") # Environment name (e.g., dev, staging, prod)
  })

  description = "Global configuration values for AWS environment"

  default = {}

  validation {
    condition     = can(regex("^\\d{12}$", var.global_values.destination_account_id))
    error_message = "DestinationAccountId must be 12 digits"
  }

  validation {
    condition     = can(regex("^((\\d{12})\\,?)*$", var.global_values.source_account_ids))
    error_message = "SourceAccountIds must be comma-separated 12-digit account IDs"
  }

  validation {
    condition     = var.global_values.quicksight_user != null
    error_message = "The quicksight_user value must be provided."
  }

  validation {
    condition     = var.global_values.cid_cfn_version == "" || can(regex("^\\d+\\.\\d+\\.\\d+$", var.global_values.cid_cfn_version))
    error_message = "The cid_cfn_version must be in the format X.Y.Z where X, Y, and Z are digits (e.g., 0.5.0)"
  }

  validation {
    condition     = var.global_values.data_export_version == "" || can(regex("^\\d+\\.\\d+\\.\\d+$", var.global_values.data_export_version))
    error_message = "The data_export_version must be in the format X.Y.Z where X, Y, and Z are digits (e.g., 4.1.5)"
  }

  validation {
    condition     = contains(["dev", "staging", "prod"], var.global_values.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

variable "destination_role_arn" {
  description = "ARN of the role to assume in the destination account"
  type        = string
  default     = null
}
