variable "name_prefix" {
  description = "Prefix for role names."
  type        = string
  default     = "fleetstream"
}

variable "bucket_arn" {
  description = "ARN of the lakehouse bucket."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the lakehouse KMS key."
  type        = string
}

variable "region" {
  description = "AWS region, used to build Glue and Athena ARNs."
  type        = string
}

variable "database_prefix" {
  description = "Glue database prefix."
  type        = string
  default     = "fleetstream"
}

variable "trusted_service" {
  description = "Service principal permitted to assume these roles (e.g. emr-serverless.amazonaws.com)."
  type        = string
  default     = "emr-serverless.amazonaws.com"
}

variable "tags" {
  type    = map(string)
  default = {}
}
