output "bucket_id" {
  description = "Name of the lakehouse bucket."
  value       = aws_s3_bucket.lakehouse.id
}

output "bucket_arn" {
  description = "ARN of the lakehouse bucket."
  value       = aws_s3_bucket.lakehouse.arn
}
