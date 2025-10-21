output "stack_outputs" {
  description = "CloudFormation stack outputs (map of strings)"
  value       = aws_cloudformation_stack.cid.outputs
}