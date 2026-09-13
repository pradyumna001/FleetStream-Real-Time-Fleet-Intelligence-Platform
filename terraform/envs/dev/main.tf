# FleetStream dev environment.
#
# Wiring only: every resource lives in a module so the same composition can be reused
# for staging and production with different variables. Module order follows the
# dependency chain - KMS first, because both storage and Kafka encrypt with that key.

locals {
  name_prefix = "fleetstream-${var.environment}"

  tags = {
    Project     = "fleetstream"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

module "kms" {
  source = "../../modules/kms"

  name = "${local.name_prefix}-lakehouse"
  tags = local.tags
}

module "s3" {
  source = "../../modules/s3"

  bucket_name = var.bucket_name
  kms_key_arn = module.kms.key_arn
  tags        = local.tags
}

module "glue" {
  source = "../../modules/glue"

  database_prefix    = replace(local.name_prefix, "-", "_")
  warehouse_location = "s3://${module.s3.bucket_id}/warehouse"
  tags               = local.tags
}

module "iam" {
  source = "../../modules/iam"

  name_prefix     = local.name_prefix
  bucket_arn      = module.s3.bucket_arn
  kms_key_arn     = module.kms.key_arn
  region          = var.region
  database_prefix = replace(local.name_prefix, "-", "_")
  tags            = local.tags
}

module "msk" {
  source = "../../modules/msk"

  cluster_name       = local.name_prefix
  subnet_ids         = var.subnet_ids
  security_group_ids = var.msk_security_group_ids
  kms_key_arn        = module.kms.key_arn
  tags               = local.tags
}
