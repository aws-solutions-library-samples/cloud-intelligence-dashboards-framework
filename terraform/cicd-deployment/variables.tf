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
    cur_version = string

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
    lake_formation_enabled               = "no"

    # CUR Parameters
    cur_version = "2.0"

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
