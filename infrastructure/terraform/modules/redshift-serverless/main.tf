# =============================================================================
# modules/redshift-serverless/main.tf
# =============================================================================
# Redshift Serverless namespace + workgroup with:
#   - IAM role for Iceberg Spectrum (S3 + Glue catalog access)
#   - KMS encryption at rest
#   - VPC network isolation
#   - CloudWatch usage alarms (RPU > 400, query duration > 300s)
# =============================================================================

locals {
  name_prefix = "${var.project}-${var.environment}"
}

# ── IAM role for Redshift Serverless ─────────────────────────────────────────

resource "aws_iam_role" "redshift" {
  name = "${local.name_prefix}-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "redshift_data_access" {
  name = "${local.name_prefix}-redshift-data-access"
  role = aws_iam_role.redshift.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseRead"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*",
        ]
      },
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey"]
        Resource = [var.kms_key_arn]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions",
          "glue:GetPartition",
        ]
        Resource = ["*"]
      },
      {
        Sid    = "LakeFormation"
        Effect = "Allow"
        Action = ["lakeformation:GetDataAccess"]
        Resource = ["*"]
      }
    ]
  })
}

# ── Redshift Serverless namespace ─────────────────────────────────────────────

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name       = var.namespace_name
  admin_username       = var.admin_username
  admin_user_password  = var.admin_password
  db_name              = var.database_name
  default_iam_role_arn = aws_iam_role.redshift.arn
  iam_roles            = [aws_iam_role.redshift.arn]
  kms_key_id           = var.kms_key_arn

  log_exports = ["userlog", "connectionlog", "useractivitylog"]

  tags = var.tags
}

# ── Redshift Serverless workgroup ─────────────────────────────────────────────

resource "aws_redshiftserverless_workgroup" "main" {
  workgroup_name = var.workgroup_name
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name

  base_capacity    = var.base_capacity_rpu
  max_capacity     = var.max_capacity_rpu
  enhanced_vpc_routing = true
  publicly_accessible  = false

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.redshift_sg_id]

  config_parameter {
    parameter_key   = "enable_user_activity_logging"
    parameter_value = "true"
  }
  config_parameter {
    parameter_key   = "query_group"
    parameter_value = "default"
  }
  config_parameter {
    parameter_key   = "search_path"
    parameter_value = "gold,silver,public"
  }

  tags = var.tags

  depends_on = [aws_redshiftserverless_namespace.main]
}

# ── Usage alarms ──────────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "rpu_high" {
  alarm_name          = "${local.name_prefix}-redshift-rpu-high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ComputeCapacityUsed"
  namespace           = "AWS/Redshift-Serverless"
  period              = 60
  statistic           = "Maximum"
  threshold           = 400
  alarm_description   = "Redshift RPU usage > 400 — consider query optimisation or RPU increase"

  dimensions = {
    Workgroup = aws_redshiftserverless_workgroup.main.workgroup_name
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "query_duration_high" {
  alarm_name          = "${local.name_prefix}-redshift-long-queries"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "QueryDuration"
  namespace           = "AWS/Redshift-Serverless"
  period              = 300
  statistic           = "p95"
  threshold           = 300000   # 300s in ms
  alarm_description   = "p95 query duration > 5 minutes — check slow query log"

  dimensions = {
    Workgroup = aws_redshiftserverless_workgroup.main.workgroup_name
  }

  tags = var.tags
}

# ── Glue connection for dbt-glue (Redshift spectrum access) ──────────────────

resource "aws_glue_connection" "redshift" {
  name            = "${local.name_prefix}-redshift-connection"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshiftserverless_workgroup.main.endpoint[0].address}:5439/${var.database_name}"
    USERNAME            = var.admin_username
    PASSWORD            = var.admin_password
  }

  physical_connection_requirements {
    subnet_id              = var.private_subnet_ids[0]
    security_group_id_list = [var.redshift_sg_id]
    availability_zone      = data.aws_subnet.first.availability_zone
  }
}

data "aws_subnet" "first" {
  id = var.private_subnet_ids[0]
}
