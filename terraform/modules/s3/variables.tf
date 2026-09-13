variable "bucket_name" {
  description = "Name of the lakehouse bucket."
  type        = string
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key used for server-side encryption."
  type        = string
}

variable "raw_retention_days" {
  description = "Days before raw telemetry transitions to infrequent access."
  type        = number
  default     = 30
}

variable "raw_glacier_days" {
  description = "Days before raw telemetry transitions to Glacier Instant Retrieval."
  type        = number
  default     = 90
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
