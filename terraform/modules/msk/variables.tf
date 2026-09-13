variable "cluster_name" {
  type    = string
  default = "fleetstream"
}

variable "kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "broker_count" {
  description = "Must be a multiple of the number of subnets."
  type        = number
  default     = 3
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "broker_volume_gb" {
  type    = number
  default = 500
}

variable "subnet_ids" {
  description = "Private subnets, one per availability zone."
  type        = list(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "kms_key_arn" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
