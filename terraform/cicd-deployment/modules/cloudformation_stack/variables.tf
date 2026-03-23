variable "config" {
  description = "Configuration object for the CloudFormation stack"
  type = object({
    enabled      = bool
    stack_name   = string
    template_url = string
    capabilities = list(string)
    parameters   = map(string)
    timeouts = object({
      create = string
      update = string
      delete = string
    })
    tags = map(string)
  })
}