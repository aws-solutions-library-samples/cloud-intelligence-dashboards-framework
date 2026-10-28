# Foundational stacks - These are foundational stacks and should exist first
# They provide the core infrastructure required for all dashboards

# 1. Data exports destination stack - Creates destination account resources
module "cid_dataexports_destination" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config = local.foundational_stacks.cid_dataexports_destination
}

# 2. Data exports source stack - Creates source account resources (conditional)
module "cid_dataexports_source" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.management
  }

  config     = local.foundational_stacks.cid_dataexports_source
  depends_on = [module.cid_dataexports_destination]
}

# 3. Cloud Intelligence Dashboards - Main dashboard stack with foundational dashboards
module "cloud_intelligence_dashboards" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config = local.foundational_stacks.cloud_intelligence_dashboards
  depends_on = [
    module.cid_dataexports_source,
    module.cid_dataexports_destination
  ]
}

# Additional CUR-based dashboards - Sequential deployment
# These are optional dashboards that deploy after foundational stacks
# They must deploy sequentially to avoid CloudFormation conflicts

# 1. Trends Dashboard - Daily anomaly detection and cost trends analysis
module "trends_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.trends
  depends_on = [module.cloud_intelligence_dashboards]
}

# 2. Data Transfer Dashboard - Data transfer cost analysis and optimization
module "datatransfer_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.datatransfer
  depends_on = [module.trends_dashboard]
}

# 3. Marketplace Dashboard - AWS Marketplace spend analysis
module "marketplace_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.marketplace
  depends_on = [module.datatransfer_dashboard]
}

# 4. Connect Dashboard - Amazon Connect cost insights and usage analysis
module "connect_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.connect
  depends_on = [module.marketplace_dashboard]
}

# 5. Containers Dashboard - SCAD containers cost allocation and optimization
module "containers_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.containers
  depends_on = [module.connect_dashboard]
}

# 6. FOCUS Dashboard - FinOps Open Cost and Usage Specification dashboard
module "focus_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.focus
  depends_on = [module.containers_dashboard]
}

# 7. CORA Dashboard - Cost Optimization Recommended Actions
module "cora_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.cora
  depends_on = [module.focus_dashboard]
}

# 8. Pricing Change Analysis Dashboard - Public pricing changes and their cost impact
module "pca_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.pca
  depends_on = [module.cora_dashboard]
}

# 9. Media Services Insights Dashboard - AWS Elemental media services cost and reservations
module "msih_dashboard" {
  source = "./modules/cloudformation_stack"
  providers = {
    aws = aws.datacollection
  }

  config     = local.additional_dashboards.msih
  depends_on = [module.pca_dashboard]
}

# Warn at plan time when Pricing Change Analysis is enabled with a CID version
# whose Lambda layer cannot build the CUR 2.0 proxy view it depends on.
check "pca_minimum_cid_cfn_version" {
  assert {
    condition = (
      var.dashboards.pca != "yes" ||
      coalesce(local.cid_cfn_version_number, local.pca_min_cid_cfn_version_number) >= local.pca_min_cid_cfn_version_number
    )
    error_message = "Pricing Change Analysis (dashboards.pca) requires cid_cfn_version ${local.pca_min_cid_cfn_version} or later, but cid_cfn_version is '${var.global_values.cid_cfn_version}'. The Pricing-Change-Analysis-Dashboard stack will fail with older versions."
  }
}