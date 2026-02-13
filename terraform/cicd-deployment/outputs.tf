# Outputs for cid_dataexports_destination stack
output "cid_dataexports_destination_outputs" {
  description = "Outputs from the cid_dataexports_destination stack"
  value = {
    stack_id = module.cid_dataexports_destination.stack_id
    outputs  = module.cid_dataexports_destination.stack_outputs
  }
}

# Outputs for cid_dataexports_source stack
output "cid_dataexports_source_outputs" {
  description = "Outputs from the cid_dataexports_source stack"
  value = {
    stack_id = module.cid_dataexports_source.stack_id
    outputs  = module.cid_dataexports_source.stack_outputs
  }
}

# Outputs for cloud_intelligence_dashboards stack
output "cloud_intelligence_dashboards_outputs" {
  description = "Outputs from the cloud_intelligence_dashboards stack"
  value = {
    stack_id = module.cloud_intelligence_dashboards.stack_id
    outputs  = module.cloud_intelligence_dashboards.stack_outputs
  }
}

# Dashboard outputs
output "dashboard_summary" {
  description = "Summary of all deployed dashboards"
  value = {
    foundational = {
      for k, v in {
        cudos_v5          = var.dashboards.cudos_v5
        cost_intelligence = var.dashboards.cost_intelligence
        kpi               = var.dashboards.kpi
      } : k => v if v == "yes"
    }
    additional = {
      for k, v in local.enabled_additional_dashboards : k => {
        dashboard_id = v.dashboard_id
        dataset_name = v.dataset_name
      }
    }
  }
}

# Additional dashboards outputs
output "additional_dashboards_stacks" {
  description = "Additional dashboard CloudFormation stacks"
  value = {
    trends = var.dashboards.trends == "yes" ? {
      stack_id = module.trends_dashboard.stack_id
      outputs  = module.trends_dashboard.stack_outputs
    } : null
    datatransfer = var.dashboards.datatransfer == "yes" ? {
      stack_id = module.datatransfer_dashboard.stack_id
      outputs  = module.datatransfer_dashboard.stack_outputs
    } : null
    marketplace = var.dashboards.marketplace == "yes" ? {
      stack_id = module.marketplace_dashboard.stack_id
      outputs  = module.marketplace_dashboard.stack_outputs
    } : null
    connect = var.dashboards.connect == "yes" ? {
      stack_id = module.connect_dashboard.stack_id
      outputs  = module.connect_dashboard.stack_outputs
    } : null
    containers = var.dashboards.containers == "yes" ? {
      stack_id = module.containers_dashboard.stack_id
      outputs  = module.containers_dashboard.stack_outputs
    } : null
  }
}
