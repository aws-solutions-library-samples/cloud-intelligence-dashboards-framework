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

# Dashboard outputs
output "dashboard_summary" {
  description = "Summary of all deployed dashboards"
  value = {
    foundational = {
      for k, v in local.foundational_dashboards : k => v.enabled if v.enabled == "yes"
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
    trends = length(aws_cloudformation_stack.trends_dashboard) > 0 ? {
      stack_id = aws_cloudformation_stack.trends_dashboard[0].id
      outputs  = aws_cloudformation_stack.trends_dashboard[0].outputs
    } : null
    datatransfer = length(aws_cloudformation_stack.datatransfer_dashboard) > 0 ? {
      stack_id = aws_cloudformation_stack.datatransfer_dashboard[0].id
      outputs  = aws_cloudformation_stack.datatransfer_dashboard[0].outputs
    } : null
    marketplace = length(aws_cloudformation_stack.marketplace_dashboard) > 0 ? {
      stack_id = aws_cloudformation_stack.marketplace_dashboard[0].id
      outputs  = aws_cloudformation_stack.marketplace_dashboard[0].outputs
    } : null
    connect = length(aws_cloudformation_stack.connect_dashboard) > 0 ? {
      stack_id = aws_cloudformation_stack.connect_dashboard[0].id
      outputs  = aws_cloudformation_stack.connect_dashboard[0].outputs
    } : null
    containers = length(aws_cloudformation_stack.containers_dashboard) > 0 ? {
      stack_id = aws_cloudformation_stack.containers_dashboard[0].id
      outputs  = aws_cloudformation_stack.containers_dashboard[0].outputs
    } : null
  }
}
