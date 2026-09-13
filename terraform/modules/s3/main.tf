# The lakehouse bucket: raw telemetry, Iceberg warehouse and checkpoints.

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
  tags   = var.tags

  lifecycle {
    # The raw layer is the replay source for the entire platform. Losing it makes
    # every backfill and every reprocessing impossible, so an accidental destroy
    # must be blocked at the tooling level rather than trusted to review.
    prevent_destroy = true
  }
}

# Versioning protects against an overwrite bug in a streaming job silently
# destroying history. It costs storage; that is the cheaper side of the trade.
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    # S3 Bucket Keys cut KMS request costs substantially on a bucket written to
    # this often - every Parquet file would otherwise be a separate KMS call.
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Raw telemetry is read constantly for a few weeks, then rarely - but it must stay
  # retrievable, because it is what backfills replay from. Tiering rather than expiry.
  rule {
    id     = "raw-tiering"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = var.raw_retention_days
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = var.raw_glacier_days
      storage_class = "GLACIER_IR"
    }
  }

  # Iceberg expires its own snapshots, which leaves noncurrent versions behind.
  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {
      prefix = "warehouse/"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  # Checkpoints are worthless once a query has moved past them, and they accumulate
  # many small objects.
  rule {
    id     = "checkpoint-cleanup"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  # A failed multipart upload is invisible in the console but still billed.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
