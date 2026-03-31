locals {
  name_prefix = "${var.project}-${var.environment}"

  flink_common_props = {
    "parallelism.default"                 = tostring(var.parallelism)
    "execution.checkpointing.interval"    = tostring(var.checkpoint_interval_ms)
    "execution.checkpointing.mode"        = "EXACTLY_ONCE"
    "state.backend"                       = "rocksdb"
    "state.backend.incremental"           = "true"
    "state.checkpoints.dir"              = "s3://${var.flink_checkpoints_bucket}/checkpoints"
    "state.savepoints.dir"               = "s3://${var.flink_checkpoints_bucket}/savepoints"
    "restart-strategy"                    = "exponential-delay"
    "restart-strategy.exponential-delay.initial-backoff" = "10 s"
    "restart-strategy.exponential-delay.max-backoff"     = "5 min"
  }
}

resource "aws_iam_role" "flink" {
  name = "${local.name_prefix}-flink-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "flink_data_access" {
  name = "${local.name_prefix}-flink-data-access"
  role = aws_iam_role.flink.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3CheckpointAccess"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:AbortMultipartUpload"]
        Resource = [
          var.flink_checkpoints_bucket_arn,
          "${var.flink_checkpoints_bucket_arn}/*",
          var.lakehouse_bucket_arn,
          "${var.lakehouse_bucket_arn}/*",
        ]
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = ["kms:GenerateDataKey", "kms:Decrypt", "kms:Encrypt", "kms:DescribeKey"]
        Resource = [var.kms_key_arn]
      },
      {
        Sid    = "MSKAccess"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup",
        ]
        Resource = [
          var.msk_cluster_arn,
          "arn:aws:kafka:${var.aws_region}:${var.aws_account_id}:topic/*",
          "arn:aws:kafka:${var.aws_region}:${var.aws_account_id}:group/*",
        ]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions", "glue:UpdateTable"]
        Resource = ["*"]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
        Resource = ["arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/kinesis-analytics/*"]
      },
      {
        Sid    = "VPCAccess"
        Effect = "Allow"
        Action = ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeNetworkInterfaces", "ec2:CreateNetworkInterface", "ec2:CreateNetworkInterfacePermission", "ec2:DeleteNetworkInterface"]
        Resource = ["*"]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = [var.sns_alert_topic_arn]
      },
      {
        Sid    = "SageMakerRuntime"
        Effect = "Allow"
        Action = ["sagemaker:InvokeEndpoint"]
        Resource = ["arn:aws:sagemaker:${var.aws_region}:${var.aws_account_id}:endpoint/*"]
      },
      {
        Sid    = "FeatureStoreRuntime"
        Effect = "Allow"
        Action = ["sagemaker:PutRecord", "sagemaker:GetRecord"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "flink" {
  for_each          = toset(["bronze-writer", "session-stitcher", "fraud-scorer", "churn-enrichment"])
  name              = "/aws/kinesis-analytics/${local.name_prefix}-${each.key}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_kinesisanalyticsv2_application" "bronze_writer" {
  name                   = "${local.name_prefix}-bronze-writer"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = var.lakehouse_bucket_arn
          file_key   = "flink-apps/bronze_writer.zip"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = 5000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }
      parallelism_configuration {
        configuration_type = "CUSTOM"
        parallelism        = var.parallelism
        auto_scaling_enabled = true
      }
    }

    vpc_configuration {
      subnet_ids         = var.private_subnet_ids
      security_group_ids = [var.flink_security_group_id]
    }

    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = merge(local.flink_common_props, {
          "MSK_BROKERS"          = var.msk_brokers
          "SCHEMA_REGISTRY_URL"  = var.schema_registry_url
          "LAKEHOUSE_BUCKET"     = var.lakehouse_bucket_name
        })
      }
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = "${aws_cloudwatch_log_group.flink["bronze-writer"].arn}:*"
  }

  tags = var.tags
}

resource "aws_kinesisanalyticsv2_application" "session_stitcher" {
  name                   = "${local.name_prefix}-session-stitcher"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = var.lakehouse_bucket_arn
          file_key   = "flink-apps/session_stitcher.zip"
        }
      }
    }
    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = 5000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.parallelism
        auto_scaling_enabled = true
      }
    }
    vpc_configuration {
      subnet_ids         = var.private_subnet_ids
      security_group_ids = [var.flink_security_group_id]
    }
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = merge(local.flink_common_props, {
          "MSK_BROKERS"         = var.msk_brokers
          "SCHEMA_REGISTRY_URL" = var.schema_registry_url
          "LAKEHOUSE_BUCKET"    = var.lakehouse_bucket_name
        })
      }
    }
  }
  cloudwatch_logging_options {
    log_stream_arn = "${aws_cloudwatch_log_group.flink["session-stitcher"].arn}:*"
  }
  tags = var.tags
}

resource "aws_kinesisanalyticsv2_application" "fraud_scorer" {
  name                   = "${local.name_prefix}-fraud-scorer"
  runtime_environment    = "FLINK-1_19"
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = var.lakehouse_bucket_arn
          file_key   = "flink-apps/fraud_scorer.zip"
        }
      }
    }
    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = 5000
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.parallelism
        auto_scaling_enabled = true
      }
    }
    vpc_configuration {
      subnet_ids         = var.private_subnet_ids
      security_group_ids = [var.flink_security_group_id]
    }
    environment_properties {
      property_group {
        property_group_id = "FlinkApplicationProperties"
        property_map = merge(local.flink_common_props, {
          "MSK_BROKERS"         = var.msk_brokers
          "SCHEMA_REGISTRY_URL" = var.schema_registry_url
          "SNS_TOPIC_ARN"       = var.sns_alert_topic_arn
        })
      }
    }
  }
  cloudwatch_logging_options {
    log_stream_arn = "${aws_cloudwatch_log_group.flink["fraud-scorer"].arn}:*"
  }
  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "flink_downtime" {
  for_each = {
    "bronze-writer"    = aws_kinesisanalyticsv2_application.bronze_writer.name
    "session-stitcher" = aws_kinesisanalyticsv2_application.session_stitcher.name
    "fraud-scorer"     = aws_kinesisanalyticsv2_application.fraud_scorer.name
  }

  alarm_name          = "${local.name_prefix}-flink-${each.key}-downtime"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "uptime"
  namespace           = "AWS/KinesisAnalytics"
  period              = 60
  statistic           = "Minimum"
  threshold           = 1
  alarm_description   = "Flink app ${each.key} is not running"
  alarm_actions       = [var.sns_alert_topic_arn]
  dimensions          = { Application = each.value }
  tags                = var.tags
}
