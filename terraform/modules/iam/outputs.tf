output "spark_role_arn" {
  description = "Role assumed by the Spark streaming and batch jobs."
  value       = aws_iam_role.spark.arn
}

output "transform_role_arn" {
  description = "Role assumed by dbt."
  value       = aws_iam_role.transform.arn
}

output "analyst_role_arn" {
  description = "Read-only role for analysts and BI tools."
  value       = aws_iam_role.analyst.arn
}
