data "aws_s3_bucket" "template_bucket" {
  bucket = var.template_bucket
}

resource "aws_s3_object" "template" {
  bucket      = data.aws_s3_bucket.template_bucket.bucket
  key         = var.template_key
  source      = "${path.module}/../../cfn-templates/cid-cfn.yml"
  source_hash = filemd5("${path.module}/../../cfn-templates/cid-cfn.yml")
  tags        = var.stack_tags
}

resource "aws_cloudformation_stack" "cid" {
  name         = var.stack_name
  template_url = "https://${data.aws_s3_bucket.template_bucket.bucket_regional_domain_name}/${aws_s3_object.template.key}?hash=${aws_s3_object.template.source_hash}"
  capabilities = ["CAPABILITY_NAMED_IAM"]
  parameters   = var.stack_parameters
  iam_role_arn = var.stack_iam_role
  policy_body  = var.stack_policy_body
  policy_url   = var.stack_policy_url
  # checkov:skip=CKV_AWS_124:Stack event notifications are configurable by the user
  notification_arns = var.stack_notification_arns
  tags              = var.stack_tags
}

