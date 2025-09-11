locals {
  # Get destination_role_arn from TF_VAR environment variable
  destination_role_arn = var.destination_role_arn

  # # Create an effective global_values with the potentially overridden destination_role_arn
  # effective_global_values = merge(var.global_values, {
  #   destination_role_arn = local.destination_role_arn != "" ? local.destination_role_arn : var.global_values.destination_role_arn
  # })

  # Common CloudFormation template parameters
  common_template_url_base = "https://aws-managed-cost-intelligence-dashboards.s3.amazonaws.com/cfn"

  # Common CloudFormation capabilities
  common_capabilities = ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]

  # Template validation
  template_urls = {
    data_exports = "${local.common_template_url_base}/data-exports/${var.global_values.data_export_version}/data-exports-aggregation.yaml"
    cudos        = "${local.common_template_url_base}/${var.global_values.cid_cfn_version}/cid-cfn.yml"
    cid_plugin   = "${local.common_template_url_base}/${var.global_values.cid_cfn_version}/cid-plugin.yml"
  }

  # Dashboard configurations
  foundational_dashboards = {
    cudos_v5 = {
      parameter_name = "DeployCUDOSv5"
      enabled        = var.dashboards.cudos_v5
    }
    cost_intelligence = {
      parameter_name = "DeployCostIntelligenceDashboard"
      enabled        = var.dashboards.cost_intelligence
    }
    kpi = {
      parameter_name = "DeployKPIDashboard"
      enabled        = var.dashboards.kpi
    }
  }

  additional_dashboards = {
    trends = {
      stack_name   = "Trends-Dashboard"
      dashboard_id = "trends-dashboard"
      dataset_name = "daily-anomaly-detection"
      parameters   = {}
      enabled      = var.dashboards.trends
    }
    datatransfer = {
      stack_name   = "DataTransfer-Cost-Analysis-Dashboard"
      dashboard_id = "datatransfer-cost-analysis-dashboard"
      dataset_name = "data_transfer_view"
      parameters   = {}
      enabled      = var.dashboards.datatransfer
    }
    marketplace = {
      stack_name   = "AWS-Marketplace-SPG-Dashboard"
      dashboard_id = "aws-marketplace"
      dataset_name = "marketplace_view"
      parameters   = { RequiresDataCollection = "no" }
      enabled      = var.dashboards.marketplace
    }
    connect = {
      stack_name   = "Amazon-Connect-Cost-Insight-Dashboard"
      dashboard_id = "amazon-connect-cost-insight-dashboard"
      dataset_name = "resource_connect_view"
      parameters   = {}
      enabled      = var.dashboards.connect
    }
    containers = {
      stack_name   = "SCAD-Containers-Dashboard"
      dashboard_id = "scad-containers-cost-allocation"
      dataset_name = "scad_cca_summary_view"
      parameters   = {}
      enabled      = var.dashboards.containers
    }
  }

  enabled_additional_dashboards = {
    for k, v in local.additional_dashboards : k => v if v.enabled == "yes"
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


}
