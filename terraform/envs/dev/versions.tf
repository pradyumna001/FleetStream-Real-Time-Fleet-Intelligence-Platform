terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }

  # No backend block is committed on purpose.
  #
  # CI runs `terraform init -backend=false` to validate the configuration without
  # credentials or state access. A real deployment adds an S3 backend with DynamoDB
  # locking here - state must never be local for shared infrastructure, because two
  # concurrent applies against local state silently corrupt each other.
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}
