data "aws_caller_identity" "this" {}
data "aws_partition" "this" {}
data "aws_region" "this" {}

###
# Aggregation S3 bucket
###
resource "aws_s3_bucket" "this" {
  # checkov:skip=CKV2_AWS_62:Due to dependencies, S3 event notifications must be configured external to the module
  # checkov:skip=CKV_AWS_144:CUR data can be backfilled on demand. Cross-region replication is not needed.
  bucket        = "${var.resource_prefix}-${data.aws_caller_identity.this.account_id}-shared"
  force_destroy = true

  tags = var.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  # checkov:skip=CKV2_AWS_67:KMS Key rotation is not in scope for this module as we do not create the key
  bucket = aws_s3_bucket.this.bucket
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = var.kms_key_id
      sse_algorithm     = var.kms_key_id != null ? "aws:kms" : "AES256"
    }
  }
}

resource "aws_s3_bucket_logging" "this" {
  count = var.s3_access_logging.enabled ? 1 : 0

  bucket        = aws_s3_bucket.this.bucket
  target_bucket = var.s3_access_logging.bucket
  target_prefix = var.s3_access_logging.prefix
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.bucket
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.bucket
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.bucket
  rule {
    id     = "Object&Version Expiration"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 32
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "bucket_policy" {
  policy_id = "CrossAccessPolicy"
  statement {
    sid     = "AllowTLS12Only"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]
    condition {
      test     = "NumericLessThan"
      variable = "s3:TlsVersion"
      values   = [1.2]
    }
  }
  statement {
    sid     = "AllowOnlyHTTPS"
    effect  = "Deny"
    actions = ["s3:*"]
    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
    resources = [
      aws_s3_bucket.this.arn,
      "${aws_s3_bucket.this.arn}/*",
    ]
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = [false]
    }
  }
  statement {
    sid    = "AllowReplicationWrite"
    effect = "Allow"
    actions = [
      "s3:ReplicateObject",
      "s3:ReplicateDelete",
    ]
    principals {
      type        = "AWS"
      identifiers = var.source_account_ids
    }
    resources = [
      "${aws_s3_bucket.this.arn}/*",
    ]
  }
  statement {
    sid    = "AllowReplicationRead"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:ListBucketVersions",
      "s3:GetBucketVersioning",
    ]
    principals {
      type        = "AWS"
      identifiers = var.source_account_ids
    }
    resources = [
      aws_s3_bucket.this.arn,
    ]
  }
  # Only add these statements if we are creating a local CUR in the destination account
  dynamic "statement" {
    for_each = var.create_cur ? [1] : []
    content {
      sid    = "AllowReadBilling"
      effect = "Allow"
      actions = [
        "s3:GetBucketAcl",
        "s3:GetBucketPolicy",
      ]
      principals {
        type        = "Service"
        identifiers = ["billingreports.amazonaws.com"]
      }
      resources = [
        aws_s3_bucket.this.arn,
        "${aws_s3_bucket.this.arn}/*",
      ]
      condition {
        test     = "StringLike"
        values   = ["arn:${data.aws_partition.this.partition}:cur:us-east-1:${data.aws_caller_identity.this.account_id}:definition/*"]
        variable = "aws:SourceArn"
      }
      condition {
        test     = "StringEquals"
        values   = [data.aws_caller_identity.this.account_id]
        variable = "aws:SourceAccount"
      }
    }
  }
  dynamic "statement" {
    for_each = var.create_cur ? [1] : []
    content {
      sid    = "AllowWriteBilling"
      effect = "Allow"
      actions = [
        "s3:PutObject",
      ]
      principals {
        type        = "Service"
        identifiers = ["billingreports.amazonaws.com"]
      }
      resources = [
        "${aws_s3_bucket.this.arn}/*",
      ]
      condition {
        test     = "StringLike"
        values   = ["arn:${data.aws_partition.this.partition}:cur:us-east-1:${data.aws_caller_identity.this.account_id}:definition/*"]
        variable = "aws:SourceArn"
      }
      condition {
        test     = "StringEquals"
        values   = [data.aws_caller_identity.this.account_id]
        variable = "aws:SourceAccount"
      }
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.bucket_policy.json
}

###
# CUR
###
resource "aws_cur_report_definition" "this" {
  provider = aws.useast1
  count    = var.create_cur ? 1 : 0

  # Make sure versioning and bucket policy is configured first
  depends_on = [
    aws_s3_bucket_versioning.this,
    aws_s3_bucket_policy.this
  ]

  report_name                = "${var.resource_prefix}-${var.cur_name_suffix}"
  time_unit                  = "HOURLY"
  format                     = "Parquet"
  compression                = "Parquet"
  additional_schema_elements = var.enable_split_cost_allocation_data ? ["RESOURCES", "SPLIT_COST_ALLOCATION_DATA"] : ["RESOURCES"]
  s3_bucket                  = aws_s3_bucket.this.bucket
  s3_region                  = data.aws_region.this.id
  s3_prefix                  = "cur/${data.aws_caller_identity.this.account_id}"
  additional_artifacts       = ["ATHENA"]
  report_versioning          = "OVERWRITE_REPORT"
  refresh_closed_reports     = true
}
