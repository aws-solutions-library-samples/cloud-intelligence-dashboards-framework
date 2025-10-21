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
