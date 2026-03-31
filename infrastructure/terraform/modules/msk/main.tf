locals {
  name_prefix = "${var.project}-${var.environment}"
}

resource "aws_msk_configuration" "main" {
  name              = "${local.name_prefix}-msk-config"
  kafka_versions    = [var.kafka_version]
  description       = "PulseCommerce MSK configuration"

  server_properties = <<-EOF
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=12
    log.retention.hours=168
    log.segment.bytes=536870912
    log.cleanup.policy=delete
    compression.type=lz4
    replica.fetch.max.bytes=10485760
    message.max.bytes=10485760
    # MSK Tiered Storage
    remote.log.storage.system.enable=${var.tiered_storage_enabled}
    log.local.retention.ms=86400000
    log.local.retention.bytes=-1
  EOF
}

resource "aws_msk_cluster" "main" {
  cluster_name           = "${local.name_prefix}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.private_subnet_ids
    security_groups = [var.msk_security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_gb
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
    encryption_at_rest_kms_key_arn = var.kms_key_arn
  }

  client_authentication {
    sasl {
      iam   = true
      scram = false
    }
    tls {}
  }

  open_monitoring {
    prometheus {
      jmx_exporter  { enabled_in_broker = true }
      node_exporter { enabled_in_broker = true }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk_broker.name
      }
      s3 {
        enabled = false
      }
    }
  }

  tags = merge(var.tags, { Name = "${local.name_prefix}-msk" })
}

resource "aws_cloudwatch_log_group" "msk_broker" {
  name              = "/aws/msk/${local.name_prefix}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${local.name_prefix}-msk-consumer-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "SumOffsetLag"
  namespace           = "AWS/Kafka"
  period              = 60
  statistic           = "Maximum"
  threshold           = 100000
  alarm_description   = "MSK consumer group lag exceeds 100k — check Flink job health"
  alarm_actions       = [var.sns_alert_topic_arn]
  ok_actions          = [var.sns_alert_topic_arn]

  dimensions = {
    Cluster  = aws_msk_cluster.main.cluster_name
    Consumer = "flink-bronze-writer"
  }

  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "disk_used" {
  alarm_name          = "${local.name_prefix}-msk-disk-used"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "KafkaDataLogsDiskUsed"
  namespace           = "AWS/Kafka"
  period              = 300
  statistic           = "Maximum"
  threshold           = 80
  alarm_description   = "MSK broker disk > 80% — expand storage or reduce retention"
  alarm_actions       = [var.sns_alert_topic_arn]

  dimensions = {
    Cluster = aws_msk_cluster.main.cluster_name
  }

  tags = var.tags
}

resource "aws_iam_policy" "msk_producer_consumer" {
  name        = "${local.name_prefix}-msk-producer-consumer"
  description = "Allows Kafka produce/consume via IAM auth on PulseCommerce MSK"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster",
        ]
        Resource = aws_msk_cluster.main.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:WriteData",
          "kafka-cluster:ReadData",
          "kafka-cluster:DescribeTopic",
        ]
        Resource = "arn:aws:kafka:${var.aws_region}:*:topic/${aws_msk_cluster.main.cluster_name}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup",
        ]
        Resource = "arn:aws:kafka:${var.aws_region}:*:group/${aws_msk_cluster.main.cluster_name}/*"
      }
    ]
  })

  tags = var.tags
}
