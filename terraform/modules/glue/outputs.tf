output "database_names" {
  description = "Created Glue database names, keyed by medallion layer."
  value       = { for k, v in aws_glue_catalog_database.layer : k => v.name }
}
