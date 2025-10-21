variable "source_account_ids" {
  type        = list(string)
  description = "List of all source accounts that will replicate CUR Data. Ex:  [12345678912,98745612312,...] (fill only on Destination Account)"
}

variable "resource_prefix" {
  type        = string
  description = "Prefix used for all named resources, including S3 Bucket"
  default     = "cid"
}

variable "create_cur" {
  type        = bool
  description = "Whether to create a local CUR in the destination account or not. Set this to true if the destination account is NOT covered in the CUR of the source accounts"
}

variable "cur_name_suffix" {
  type        = string
  description = "Suffix used to name the local CUR report if create_cur is `true`"
  default     = "cur"
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
      - If create_cur is true, the "billingreports.amazonaws.com" service must have access to encrypt S3 objects with the key ID provided
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
