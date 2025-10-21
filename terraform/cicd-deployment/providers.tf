provider "aws" {
  region = var.global_values.aws_region

  default_tags {
    tags = local.common_tags
  }
}

provider "aws" {
  alias  = "destination_account"
  region = var.global_values.aws_region

  dynamic "assume_role" {
    for_each = local.destination_role_arn != null ? [1] : []
    content {
      role_arn = local.destination_role_arn
    }
  }

  default_tags {
    tags = local.common_tags
  }
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.0.0"
}
