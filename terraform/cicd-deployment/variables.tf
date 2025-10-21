variable "cid_dataexports_destination" {
  type = object({
    # Prefix used for all named resources
    resource_prefix = string
    # Enable CUR 2.0 management
    manage_cur2 = string
    # Enable FOCUS management
    manage_focus = string
    # Enable Cost Optimization Hub management
    manage_coh = string
    # Enable Split Cost Allocation Data
    enable_scad = string
    # Path for IAM roles
    role_path = string
    # Time granularity for CUR 2.0
    time_granularity = string
  })

  description = "Configuration for data exports child account settings"

  default = {
    resource_prefix  = "cid"
    manage_cur2      = "yes"
    manage_focus     = "no"
    manage_coh       = "no"
    enable_scad      = "yes"
    role_path        = "/"
    time_granularity = "HOURLY"
  }

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
    # Prefix used for all named resources in management account
    source_resource_prefix = string
    # Enable CUR 2.0 management in management account
    source_manage_cur2 = string
    # Enable FOCUS management in management account
    source_manage_focus = string
    # Enable Cost Optimization Hub management in management account
    source_manage_coh = string
    # Enable Split Cost Allocation Data in management account
    source_enable_scad = string
    # Path for IAM roles in management account
    source_role_path = string
    # Time granularity for CUR 2.0 in management account
    source_time_granularity = string
  })

  description = "Configuration for data exports management account settings"

  default = {
    source_resource_prefix  = "cid"
    source_manage_cur2      = "yes" #
    source_manage_focus     = "no"
    source_manage_coh       = "no"
    source_enable_scad      = "yes"
    source_role_path        = "/"
    source_time_granularity = "HOURLY"
  }

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
    prerequisites_quicksight             = string
    prerequisites_quicksight_permissions = string
    lake_formation_enabled               = string

    # CUR Parameters
    cur_version                        = string
    deploy_cudos_v5                    = string
    deploy_cost_intelligence_dashboard = string
    deploy_kpi_dashboard               = string

    # Optimization Parameters
    optimization_data_collection_bucket_path = string
    deploy_tao_dashboard                     = string
    deploy_compute_optimizer_dashboard       = string
    primary_tag_name                         = string
    secondary_tag_name                       = string

    # Technical Parameters
    athena_workgroup                     = string
    athena_query_results_bucket          = string
    database_name                        = string
    glue_data_catalog                    = string
    suffix                               = string
    quicksight_data_source_role_name     = string
    quicksight_data_set_refresh_schedule = string
    lambda_layer_bucket_prefix           = string
    deploy_cudos_dashboard               = string
    data_buckets_kms_keys_arns           = string
    deployment_type                      = string
    share_dashboard                      = string

    # Legacy Parameters
    keep_legacy_cur_table = string
    cur_bucket_path       = string
    cur_table_name        = string
    permissions_boundary  = string
    role_path             = string
  })

  default = {
    # Prerequisites Variables
    prerequisites_quicksight             = "yes"
    prerequisites_quicksight_permissions = "yes"
    quicksight_user                      = null
    lake_formation_enabled               = "no"

    # CUR Parameters
    cur_version                        = "2.0"
    deploy_cudos_v5                    = "yes"
    deploy_cost_intelligence_dashboard = "yes"
    deploy_kpi_dashboard               = "yes"

    # Optimization Parameters
    optimization_data_collection_bucket_path = "s3://cid-data-{account_id}"
    deploy_tao_dashboard                     = "no"
    deploy_compute_optimizer_dashboard       = "no"
    primary_tag_name                         = "owner"
    secondary_tag_name                       = "environment"

    # Technical Parameters
    athena_workgroup                     = ""
    athena_query_results_bucket          = ""
    database_name                        = ""
    glue_data_catalog                    = "AwsDataCatalog"
    suffix                               = ""
    quicksight_data_source_role_name     = "CidQuickSightDataSourceRole"
    quicksight_data_set_refresh_schedule = ""
    lambda_layer_bucket_prefix           = "aws-managed-cost-intelligence-dashboards"
    deploy_cudos_dashboard               = "no"
    data_buckets_kms_keys_arns           = ""
    deployment_type                      = "Terraform"
    share_dashboard                      = "yes"

    # Legacy Parameters
    keep_legacy_cur_table = "no"
    cur_bucket_path       = "s3://cid-{account_id}-shared/cur/"
    cur_table_name        = ""
    permissions_boundary  = ""
    role_path             = "/"
  }
}

variable "global_values" {
  type = object({
    # AWS Account Id where DataExport will be replicated to
    destination_account_id = string
    # Comma separated list of source account IDs
    source_account_ids = string
    # AWS region where the dashboard will be deployed
    aws_region = string
    # Quicksight user to share the dashboard with
    quicksight_user = string
    # CloudFormation template using for the deployment, see description to get the semantic version number (e.g. 4.1.5) https://github.com/aws-solutions-library-samples/cloud-intelligence-dashboards-framework/blob/main/cfn-templates/cid-cfn.yml
    cid_cfn_version = string
    # CloudFormation template using for the deployment, see description to get the semantic version number (e.g. 0.5.0) https://github.com/aws-solutions-library-samples/cloud-intelligence-dashboards-data-collection/blob/main/data-exports/deploy/data-exports-aggregation.yaml
    data_export_version = string
    # Environment name (e.g., dev, staging, prod)
    environment = string
  })

  description = "Global configuration values for AWS environment"

  default = {
    destination_account_id = null
    source_account_ids     = ""
    aws_region             = ""
    quicksight_user        = null
    cid_cfn_version        = ""
    data_export_version    = ""
    environment            = ""
  }

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
