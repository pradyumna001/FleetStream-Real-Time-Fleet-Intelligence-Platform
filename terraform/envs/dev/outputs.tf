output "lakehouse_bucket" {
  description = "Lakehouse bucket name."
  value       = module.s3.bucket_id
}

output "kms_key_arn" {
  value = module.kms.key_arn
}

output "glue_databases" {
  value = module.glue.database_names
}

output "spark_role_arn" {
  value = module.iam.spark_role_arn
}

output "analyst_role_arn" {
  value = module.iam.analyst_role_arn
}

output "kafka_bootstrap_servers" {
  description = "MSK bootstrap servers (IAM/TLS)."
  value       = module.msk.bootstrap_brokers_sasl_iam
  sensitive   = true
}
