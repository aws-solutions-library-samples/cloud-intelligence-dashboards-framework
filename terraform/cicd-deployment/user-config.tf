# =============================================================================
# USER CONFIGURATION VARIABLES
# =============================================================================
# This file contains the main variables that users typically need to modify.
# Most users should only need to update values in this file.
# =============================================================================

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

variable "allow_standalone_dashboard" {
  description = "Set to true to allow deploying dashboards without their dependencies (for standalone deployments or when dependencies exist elsewhere)"
  type        = bool
  default     = false
}

variable "dashboards" {
  type = object({
    # Foundational Dashboards (recommended to deploy at least one first)
    # These create shared resources that additional dashboards may reference
    cudos_v5          = string
    cost_intelligence = string
    kpi               = string

    # Additional Dashboards (can be deployed standalone if foundational dashboards exist)
    trends       = string
    datatransfer = string
    marketplace  = string
    connect      = string
    containers   = string
  })

  description = "Dashboard deployment configuration - choose which dashboards to deploy"

  default = {
    # Foundational
    cudos_v5          = "yes"
    cost_intelligence = "yes"
    kpi               = "yes"

    # Additional
    trends       = "yes"
    datatransfer = "yes"
    marketplace  = "yes"
    connect      = "yes"
    containers   = "yes"
  }

  validation {
    condition = alltrue([
      for k, v in var.dashboards : contains(["yes", "no"], v)
    ])
    error_message = "All dashboard values must be 'yes' or 'no'"
  }

  validation {
    condition = anytrue([
      var.dashboards.cudos_v5 == "yes",
      var.dashboards.cost_intelligence == "yes",
      var.dashboards.kpi == "yes",
      var.dashboards.trends == "yes",
      var.dashboards.datatransfer == "yes",
      var.dashboards.marketplace == "yes",
      var.dashboards.connect == "yes",
      var.dashboards.containers == "yes"
    ])
    error_message = "At least one dashboard must be enabled"
  }

  # Validate foundational dashboard requirement unless explicitly overridden
  validation {
    condition = (
      var.allow_standalone_dashboard ||
      anytrue([
        var.dashboards.cudos_v5 == "yes",
        var.dashboards.cost_intelligence == "yes",
        var.dashboards.kpi == "yes"
      ]) ||
      !anytrue([
        var.dashboards.trends == "yes",
        var.dashboards.datatransfer == "yes",
        var.dashboards.marketplace == "yes",
        var.dashboards.connect == "yes",
        var.dashboards.containers == "yes"
      ])
    )
    error_message = "Additional dashboards require at least one foundational dashboard (cudos_v5, cost_intelligence, or kpi) to be enabled, OR set 'allow_standalone_dashboard = true' if foundational dashboards are already deployed elsewhere."
  }
}

variable "destination_role_arn" {
  description = "ARN of the role to assume in the destination account (optional)"
  type        = string
  default     = null
}
