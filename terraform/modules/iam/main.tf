# Least-privilege roles, one per pipeline stage.
#
# The point of splitting these is that a compromised or buggy job can only do the
# damage its own stage allows. The Spark ingestion role can write Bronze and Silver
# but cannot touch Gold; the analyst role can read Gold but cannot read raw telemetry,
# which carries per-vehicle location history. A single "data platform" role would
# collapse all of that into one blast radius.
#
# Every policy is scoped to explicit prefixes. `s3:*` on the bucket would be far
# shorter and would defeat the entire purpose of the file.

data "aws_caller_identity" "current" {}

locals {
  bucket_arn = var.bucket_arn

  common_assume_role = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = var.trusted_service
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Ingestion / processing role (Spark)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "spark" {
  name               = "${var.name_prefix}-spark"
  assume_role_policy = local.common_assume_role
  tags               = var.tags
}

resource "aws_iam_role_policy" "spark_storage" {
  name = "${var.name_prefix}-spark-storage"
  role = aws_iam_role.spark.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Listing is bucket-level and cannot be prefix-scoped by resource, so it is
        # constrained by condition instead.
        Sid      = "ListScopedPrefixes"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [local.bucket_arn]
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "raw/*", "warehouse/bronze/*", "warehouse/silver/*",
              "warehouse/platform/*", "checkpoints/*",
            ]
          }
        }
      },
      {
        Sid      = "ReadRaw"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = ["${local.bucket_arn}/raw/*"]
      },
      {
        # Write is limited to the layers this role owns. Gold is built by dbt under a
        # different role, so a bug here cannot corrupt published business data.
        Sid    = "WriteBronzeSilverPlatform"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = [
          "${local.bucket_arn}/warehouse/bronze/*",
          "${local.bucket_arn}/warehouse/silver/*",
          "${local.bucket_arn}/warehouse/platform/*",
          "${local.bucket_arn}/checkpoints/*",
        ]
      },
      {
        # Iceberg writes are encrypted with the CMK, so the role needs data-key
        # operations. GenerateDataKey is required for every new object.
        Sid    = "UseLakehouseKey"
        Effect = "Allow"
        Action = [
          "kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey",
        ]
        Resource = [var.kms_key_arn]
      },
    ]
  })
}

resource "aws_iam_role_policy" "spark_catalog" {
  name = "${var.name_prefix}-spark-catalog"
  role = aws_iam_role.spark.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:BatchCreatePartition", "glue:BatchDeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${var.database_prefix}_bronze",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${var.database_prefix}_silver",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${var.database_prefix}_platform",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${var.database_prefix}_bronze/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${var.database_prefix}_silver/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${var.database_prefix}_platform/*",
        ]
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Transformation role (dbt) - reads Silver, owns Gold
# ---------------------------------------------------------------------------

resource "aws_iam_role" "transform" {
  name               = "${var.name_prefix}-transform"
  assume_role_policy = local.common_assume_role
  tags               = var.tags
}

resource "aws_iam_role_policy" "transform_storage" {
  name = "${var.name_prefix}-transform-storage"
  role = aws_iam_role.transform.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [local.bucket_arn]
        Condition = {
          StringLike = { "s3:prefix" = ["warehouse/silver/*", "warehouse/gold/*"] }
        }
      },
      {
        Sid      = "ReadSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.bucket_arn}/warehouse/silver/*"]
      },
      {
        Sid      = "WriteGold"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.bucket_arn}/warehouse/gold/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Analyst role - Gold only, read only
# ---------------------------------------------------------------------------

resource "aws_iam_role" "analyst" {
  name               = "${var.name_prefix}-analyst"
  assume_role_policy = local.common_assume_role
  tags               = var.tags
}

resource "aws_iam_role_policy" "analyst_read" {
  name = "${var.name_prefix}-analyst-read"
  role = aws_iam_role.analyst.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [local.bucket_arn]
        Condition = {
          StringLike = { "s3:prefix" = ["warehouse/gold/*"] }
        }
      },
      {
        # Gold only. Raw and Silver carry per-vehicle GPS traces, which are personal
        # data about drivers; analysts get the aggregates, not the movement history.
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${local.bucket_arn}/warehouse/gold/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      },
      {
        Sid    = "AthenaQuery"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults", "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = ["arn:aws:athena:${var.region}:${data.aws_caller_identity.current.account_id}:workgroup/*"]
      },
    ]
  })
}
