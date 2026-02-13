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