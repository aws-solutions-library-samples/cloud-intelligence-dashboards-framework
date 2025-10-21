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
