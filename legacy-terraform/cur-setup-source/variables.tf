variable "destination_bucket_arn" {
  type        = string
  description = "Destination Bucket ARN"
}

variable "resource_prefix" {
  type        = string
  default     = "cid"
  description = "Prefix used for all named resources, including the S3 Bucket"
}

variable "cur_name_suffix" {
  type        = string
  default     = "cur"
  description = "Suffix used to name the CUR report"
}

variable "s3_access_logging" {
  type = object({
    enabled = bool
    bucket  = string
    prefix  = string
  })
  description = "S3 Access Logging configuration for the CUR bucket"
  default = {
    enabled = false
    bucket  = null
    prefix  = null
  }
}

variable "kms_key_id" {
  type        = string
  default     = null
  description = <<-EOF
    !!!WARNING!!! EXPERIMENTAL - Do not use unless you know what you are doing. The correct key policies and IAM permissions
    on the S3 replication role must be configured external to this module.
      - The "billingreports.amazonaws.com" service must have access to encrypt objects with the key ID provided
      - See https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-config-for-kms-objects.html for information
        on permissions required for replicating KMS-encrypted objects
  EOF
}

variable "enable_split_cost_allocation_data" {
  type        = bool
  description = "Enable split cost allocation data for ECS and EKS for this CUR report"
  default     = false
}

variable "tags" {
  type        = map(string)
  description = "Map of tags to apply to module resources"
  default     = {}
}
