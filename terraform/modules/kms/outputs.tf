output "key_arn" {
  description = "ARN of the lakehouse KMS key."
  value       = aws_kms_key.lakehouse.arn
}

output "key_id" {
  description = "ID of the lakehouse KMS key."
  value       = aws_kms_key.lakehouse.key_id
}
