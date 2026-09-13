# Customer-managed key for the lakehouse.
#
# Customer-managed rather than the AWS-managed S3 key so that key policy, rotation
# and - most importantly - revocation are under our control. Revoking access to a
# CMK renders the data unreadable immediately, which is the only fast answer to a
# credential compromise.

data "aws_caller_identity" "current" {}

resource "aws_kms_key" "lakehouse" {
  description             = "FleetStream lakehouse encryption key"
  deletion_window_in_days = var.deletion_window_days
  enable_key_rotation     = true
  tags                    = var.tags

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Without this the key becomes unmanageable: only the key policy grants
        # access to a KMS key, so locking out the account root locks out everyone.
        Sid    = "EnableIAMPolicies"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.lakehouse.key_id
}
