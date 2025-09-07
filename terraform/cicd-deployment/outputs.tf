# Outputs for cid_dataexports_destination stack
output "cid_dataexports_destination_outputs" {
  description = "Outputs from the cid_dataexports_destination stack"
  value = {
    stack_id = aws_cloudformation_stack.cid_dataexports_destination.id
    outputs  = aws_cloudformation_stack.cid_dataexports_destination.outputs
  }
}

# Outputs for cid_dataexports_source stack
output "cid_dataexports_source_outputs" {
  description = "Outputs from the cid_dataexports_source stack"
  value = {
    stack_id = aws_cloudformation_stack.cid_dataexports_source.id
    outputs  = aws_cloudformation_stack.cid_dataexports_source.outputs
  }
}

# Outputs for cloud_intelligence_dashboards stack
output "cloud_intelligence_dashboards_outputs" {
  description = "Outputs from the cloud_intelligence_dashboards stack"
  value = {
    stack_id = aws_cloudformation_stack.cloud_intelligence_dashboards.id
    outputs  = aws_cloudformation_stack.cloud_intelligence_dashboards.outputs
  }
}

# Additional Dashboards outputs
output "additional_dashboards_outputs" {
  description = "Outputs from additional dashboards stacks"
  value = {
    for k, v in aws_cloudformation_stack.additional_dashboards : k => {
      stack_id = v.id
      outputs  = v.outputs
    }
  }
}

# Summary of deployed dashboards
output "deployed_dashboards" {
  description = "Summary of all deployed dashboards"
  value = {
    foundational = {
      cudos_v5                    = var.dashboards.cudos_v5
      cost_intelligence_dashboard = var.dashboards.cost_intelligence_dashboard
      kpi_dashboard               = var.dashboards.kpi_dashboard
    }
    additional = {
      trends_dashboard          = var.dashboards.trends_dashboard
      datatransfer_dashboard    = var.dashboards.datatransfer_dashboard
      marketplace_dashboard     = var.dashboards.marketplace_dashboard
      connect_dashboard         = var.dashboards.connect_dashboard
      scad_containers_dashboard = var.dashboards.scad_containers_dashboard
    }
  }
}
