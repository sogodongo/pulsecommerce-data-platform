locals {
  name_prefix = "${var.project}-${var.environment}"
}

resource "aws_glue_catalog_database" "bronze" {
  name        = var.glue_database_bronze
  description = "PulseCommerce Bronze layer — raw Iceberg tables"
  tags        = var.tags
}

resource "aws_glue_catalog_database" "silver" {
  name        = var.glue_database_silver
  description = "PulseCommerce Silver layer — cleansed, PII-masked Iceberg tables"
  tags        = var.tags
}

resource "aws_glue_catalog_database" "gold" {
  name        = var.glue_database_gold
  description = "PulseCommerce Gold layer — Kimball star schema"
  tags        = var.tags
}

resource "aws_iam_role" "glue" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_data_access" {
  name = "${local.name_prefix}-glue-data-access"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseAccess"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*",
          var.glue_scripts_bucket_arn,
          "${var.glue_scripts_bucket_arn}/*",
        ]
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = ["glue:*"]
        Resource = ["*"]
      },
      {
        Sid    = "LakeFormationAccess"
        Effect = "Allow"
        Action = [
          "lakeformation:GetDataAccess",
          "lakeformation:GrantPermissions",
        ]
        Resource = ["*"]
      },
      {
        Sid    = "DynamoDBBookmarks"
        Effect = "Allow"
        Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
        Resource = ["arn:aws:dynamodb:${var.aws_region}:${var.aws_account_id}:table/pulsecommerce-glue-bookmarks"]
      },
      {
        Sid    = "SecretsManagerPIISalt"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = ["arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:pulsecommerce/*"]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = ["arn:aws:sns:${var.aws_region}:${var.aws_account_id}:*"]
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_dynamodb_table" "glue_bookmarks" {
  name           = "pulsecommerce-glue-bookmarks"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "job_name"

  attribute {
    name = "job_name"
    type = "S"
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption { enabled = true }
  tags = var.tags
}

locals {
  glue_common_args = {
    "--enable-metrics"               = "true"
    "--enable-spark-ui"              = "true"
    "--spark-event-logs-path"        = "s3://${var.glue_scripts_bucket}/spark-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"                 = "python"
    "--TempDir"                      = "s3://${var.glue_scripts_bucket}/temp/"
    "--MSK_BROKERS"                  = var.msk_brokers
    "--SCHEMA_REGISTRY_URL"          = var.schema_registry_url
    "--LAKEHOUSE_BUCKET"             = var.lakehouse_bucket_name
  }
}

resource "aws_glue_job" "bronze_to_silver_events" {
  name              = "${local.name_prefix}-bronze-to-silver-events"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = var.worker_type
  number_of_workers = var.max_workers
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_scripts_bucket}/scripts/bronze_to_silver_events.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--job-bookmark-option" = "job-bookmark-enable"
  })

  execution_property { max_concurrent_runs = 1 }
  tags = var.tags
}

resource "aws_glue_job" "bronze_to_silver_orders" {
  name              = "${local.name_prefix}-bronze-to-silver-orders"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = var.worker_type
  number_of_workers = var.max_workers
  timeout           = 120

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_scripts_bucket}/scripts/bronze_to_silver_orders.py"
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--job-bookmark-option" = "job-bookmark-enable"
  })

  execution_property { max_concurrent_runs = 1 }
  tags = var.tags
}

resource "aws_glue_job" "silver_product_catalog" {
  name              = "${local.name_prefix}-silver-product-catalog"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_scripts_bucket}/scripts/silver_product_catalog.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  execution_property { max_concurrent_runs = 1 }
  tags = var.tags
}

resource "aws_glue_job" "gx_bronze_clickstream" {
  name              = "${local.name_prefix}-gx-bronze-clickstream"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_scripts_bucket}/scripts/bronze_clickstream_expectations.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  tags = var.tags
}

resource "aws_glue_job" "gx_silver_orders" {
  name              = "${local.name_prefix}-gx-silver-orders"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  command {
    name            = "glueetl"
    script_location = "s3://${var.glue_scripts_bucket}/scripts/silver_orders_expectations.py"
    python_version  = "3"
  }

  default_arguments = local.glue_common_args
  tags = var.tags
}
