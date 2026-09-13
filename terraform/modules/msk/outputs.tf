output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap servers for IAM-authenticated TLS clients."
  value       = aws_msk_cluster.fleetstream.bootstrap_brokers_sasl_iam
}

output "cluster_arn" {
  value = aws_msk_cluster.fleetstream.arn
}
