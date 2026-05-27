output "stack_id" {
  description = "ID of the CloudFormation stack"
  value       = var.config.enabled ? aws_cloudformation_stack.dashboard[0].id : null
}

output "stack_outputs" {
  description = "Outputs from the CloudFormation stack"
  value       = var.config.enabled ? aws_cloudformation_stack.dashboard[0].outputs : {}
}