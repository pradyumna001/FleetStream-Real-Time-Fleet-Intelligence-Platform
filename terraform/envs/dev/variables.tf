variable "region" {
  description = "AWS region."
  type        = string
  default     = "ap-south-1"
}

variable "environment" {
  description = "Environment name, used in resource names and tags."
  type        = string
  default     = "dev"
}

variable "bucket_name" {
  description = "Globally unique name for the lakehouse bucket."
  type        = string
  default     = "fleetstream-lakehouse-dev"
}

variable "vpc_id" {
  description = "VPC hosting the MSK cluster. Placeholder - replace before applying."
  type        = string
  default     = "vpc-placeholder"
}

variable "subnet_ids" {
  description = "Private subnets, one per AZ. Placeholders - replace before applying."
  type        = list(string)
  default     = ["subnet-placeholder-a", "subnet-placeholder-b", "subnet-placeholder-c"]
}

variable "msk_security_group_ids" {
  description = "Security groups for MSK brokers. Placeholders - replace before applying."
  type        = list(string)
  default     = ["sg-placeholder"]
}
