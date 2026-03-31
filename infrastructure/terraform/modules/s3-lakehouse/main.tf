locals {
  name_prefix = "${var.project}-${var.environment}"
}

resource "aws_kms_key" "lakehouse" {
  description             = "PulseCommerce lakehouse encryption key — ${var.environment}"
  deletion_window_in_days = var.kms_key_deletion_days
  enable_key_rotation     = true
  tags                    = merge(var.tags, { Name = "${local.name_prefix}-kms" })

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "EnableRootAccess"
        Effect = "Allow"
        Principal = { AWS = "arn:aws:iam::${var.aws_account_id}:root" }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "AllowS3Service"
        Effect = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action   = ["kms:GenerateDataKey", "kms:Decrypt"]
        Resource = "*"
      },
      {
        Sid    = "AllowGlueService"
        Effect = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action   = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:ReEncrypt*", "kms:DescribeKey"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/${local.name_prefix}-lakehouse"
  target_key_id = aws_kms_key.lakehouse.key_id
}

# aws_s3tables_table_bucket is used for S3 Tables (auto-compaction, snapshot mgmt)
# Falls back to standard S3 + Glue Iceberg if S3 Tables not available in region.

resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.lakehouse_bucket_name
  force_destroy = false
  tags          = merge(var.tags, { Name = "${local.name_prefix}-lakehouse", Layer = "all" })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true   # reduces KMS API costs
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

  rule {
    id     = "bronze-expire-raw"
    status = "Enabled"
    filter { prefix = "bronze/" }
    expiration { days = 90 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }

  rule {
    id     = "silver-transition-ia"
    status = "Enabled"
    filter { prefix = "silver/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "ml-expire-training-data"
    status = "Enabled"
    filter { prefix = "ml/training-data/" }
    expiration { days = 180 }
  }
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket        = var.glue_scripts_bucket_name
  force_destroy = false
  tags          = merge(var.tags, { Name = "${local.name_prefix}-glue-scripts" })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket" "flink_checkpoints" {
  bucket        = var.flink_checkpoints_bucket_name
  force_destroy = false
  tags          = merge(var.tags, { Name = "${local.name_prefix}-flink-checkpoints" })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lakehouse.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "flink_checkpoints" {
  bucket                  = aws_s3_bucket.flink_checkpoints.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"
    filter {}
    expiration { days = 7 }
  }
}

resource "aws_lakeformation_data_lake_settings" "main" {
  admins = [
    "arn:aws:iam::${var.aws_account_id}:root",
  ]
}

resource "aws_lakeformation_resource" "lakehouse" {
  arn      = aws_s3_bucket.lakehouse.arn
  role_arn = "arn:aws:iam::${var.aws_account_id}:role/AWSServiceRoleForLakeFormationDataAccess"
}
