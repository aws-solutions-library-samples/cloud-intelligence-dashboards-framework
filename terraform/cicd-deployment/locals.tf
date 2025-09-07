locals {
  # Get destination_role_arn from TF_VAR environment variable
  destination_role_arn = var.destination_role_arn

  # Common CloudFormation template parameters
  common_template_url_base = "https://aws-managed-cost-intelligence-dashboards.s3.amazonaws.com/cfn"

  # Common CloudFormation capabilities
  common_capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]

  # Template validation
  template_urls = {
    data_exports = "${local.common_template_url_base}/data-exports/${var.global_values.data_export_version}/data-exports-aggregation.yaml"
    cudos        = "${local.common_template_url_base}/${var.global_values.cid_cfn_version}/cid-cfn.yml"
    plugin       = "${local.common_template_url_base}/cid-plugin.yml"
  }

  # Common tags for all resources
  common_tags = {
    Environment       = var.global_values.environment
    Project           = "cloud-intelligence-dashboards"
    ManagedBy         = "terraform"
    DataExportVersion = var.global_values.data_export_version
    CidCfnVersion     = var.global_values.cid_cfn_version
  }

  # Default timeouts for CloudFormation stacks
  default_timeouts = {
    create = "30m"
    update = "30m"
    delete = "30m"
  }

  # All Dashboard configurations
  all_dashboards = {
    # Foundational Dashboards (deployed via main CFN template)
    foundational = {
      cudos_v5                    = var.dashboards.cudos_v5
      cost_intelligence_dashboard = var.dashboards.cost_intelligence_dashboard
      kpi_dashboard               = var.dashboards.kpi_dashboard
    }

    # Additional Dashboards (deployed via plugin template)
    additional = {
      trends-dashboard = {
        enabled      = var.dashboards.trends_dashboard == "yes"
        stack_name   = "Trends-Dashboard"
        dashboard_id = "trends-dashboard"
        # Actual supported parameters
        parameters = {
          DashboardId            = "trends-dashboard"
          RequiresDataCollection = "no"
          # RequiresDataExports can be added if needed
        }
      }
      datatransfer-cost-analysis-dashboard = {
        enabled      = var.dashboards.datatransfer_dashboard == "yes"
        stack_name   = "DataTransfer-Cost-Analysis-Dashboard"
        dashboard_id = "datatransfer-cost-analysis-dashboard"
        # Actual supported parameters
        parameters = {
          DashboardId            = "datatransfer-cost-analysis-dashboard"
          RequiresDataCollection = "no"
        }
      }
      aws-marketplace = {
        enabled      = var.dashboards.marketplace_dashboard == "yes"
        stack_name   = "AWS-Marketplace-SPG-Dashboard"
        dashboard_id = "aws-marketplace"
        # Actual supported parameters
        parameters = {
          DashboardId            = "aws-marketplace"
          RequiresDataCollection = "no"
        }
      }
      amazon-connect-cost-insight-dashboard = {
        enabled      = var.dashboards.connect_dashboard == "yes"
        stack_name   = "Amazon-Connect-Cost-Insight-Dashboard"
        dashboard_id = "amazon-connect-cost-insight-dashboard"
        # Actual supported parameters
        parameters = {
          DashboardId            = "amazon-connect-cost-insight-dashboard"
          RequiresDataCollection = "no"
        }
      }
      scad-containers-cost-allocation = {
        enabled      = var.dashboards.scad_containers_dashboard == "yes"
        stack_name   = "SCAD-Containers-Dashboard"
        dashboard_id = "scad-containers-cost-allocation"
        # Actual supported parameters
        parameters = {
          DashboardId            = "scad-containers-cost-allocation"
          RequiresDataCollection = "no"
        }
      }

    }
  }

  # Filter enabled additional dashboards
  enabled_additional_dashboards = {
    for k, v in local.all_dashboards.additional : k => v if v.enabled
  }

  # Check if any foundational dashboard is enabled
  foundational_enabled = (
    var.dashboards.cudos_v5 == "yes" ||
    var.dashboards.cost_intelligence_dashboard == "yes" ||
    var.dashboards.kpi_dashboard == "yes"
  )

  # Check if any additional dashboard is enabled
  additional_enabled = length(local.enabled_additional_dashboards) > 0

  # Validation: Deploy additional dashboards only if foundational dashboards are enabled
  deploy_additional = local.foundational_enabled && local.additional_enabled
}
