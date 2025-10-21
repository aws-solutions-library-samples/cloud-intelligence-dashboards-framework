output "cur_report_arn" {
  description = "ARN of the Cost and Usage Report"
  value       = aws_cur_report_definition.this.arn
}

output "cur_bucket_arn" {
  description = "ARN of the S3 Bucket where the Cost and Usage Report is delivered"
  value       = aws_s3_bucket.this.arn
}

output "cur_bucket_name" {
  description = "Name of the S3 Bucket where the Cost and Usage Report is delivered"
  value       = aws_s3_bucket.this.bucket
}

output "replication_role_arn" {
  description = "ARN of the IAM role created for S3 replication"
  value       = aws_iam_role.replication.arn
}

output "replication_role_name" {
  description = "ARN of the IAM role created for S3 replication"
  value       = aws_iam_role.replication.name
}