resource "aws_cloudformation_stack" "dashboard" {
  count = var.config.enabled ? 1 : 0

  name         = var.config.stack_name
  template_url = var.config.template_url
  capabilities = var.config.capabilities
  parameters   = var.config.parameters

  timeouts {
    create = var.config.timeouts.create
    update = var.config.timeouts.update
    delete = var.config.timeouts.delete
  }

  tags = var.config.tags
}