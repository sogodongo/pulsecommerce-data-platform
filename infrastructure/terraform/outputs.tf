output "vpc_id" {
  description = "VPC ID."
  value       = module.networking.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs."
  value       = module.networking.private_subnet_ids
}

output "lakehouse_bucket_name" {
  description = "S3 Tables (Iceberg) lakehouse bucket name."
  value       = module.s3_lakehouse.lakehouse_bucket_name
}

output "glue_scripts_bucket_name" {
  description = "Glue scripts S3 bucket name."
  value       = module.s3_lakehouse.glue_scripts_bucket_name
}

output "flink_checkpoints_bucket_name" {
  description = "Flink checkpoints S3 bucket name."
  value       = module.s3_lakehouse.flink_checkpoints_bucket_name
}

output "kms_key_arn" {
  description = "KMS key ARN used for all at-rest encryption."
  value       = module.s3_lakehouse.kms_key_arn
}

output "kms_key_id" {
  description = "KMS key ID."
  value       = module.s3_lakehouse.kms_key_id
}

output "msk_bootstrap_brokers_tls" {
  description = "MSK TLS bootstrap broker string."
  value       = module.msk.bootstrap_brokers_tls
  sensitive   = true
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN."
  value       = module.msk.cluster_arn
}

output "msk_zookeeper_connect" {
  description = "MSK ZooKeeper connection string."
  value       = module.msk.zookeeper_connect_string
  sensitive   = true
}

output "glue_role_arn" {
  description = "IAM role ARN used by Glue ELT jobs."
  value       = module.glue.glue_role_arn
}

output "glue_database_bronze" {
  value = module.glue.database_bronze
}

output "glue_database_silver" {
  value = module.glue.database_silver
}

output "glue_database_gold" {
  value = module.glue.database_gold
}

output "flink_role_arn" {
  description = "IAM role ARN used by Managed Flink applications."
  value       = module.flink.flink_role_arn
}

output "flink_bronze_writer_app_arn" {
  value = module.flink.bronze_writer_app_arn
}

output "flink_session_stitcher_app_arn" {
  value = module.flink.session_stitcher_app_arn
}

output "flink_fraud_scorer_app_arn" {
  value = module.flink.fraud_scorer_app_arn
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint."
  value       = module.redshift_serverless.workgroup_endpoint
  sensitive   = true
}

output "redshift_workgroup_arn" {
  value = module.redshift_serverless.workgroup_arn
}

output "redshift_namespace_id" {
  value = module.redshift_serverless.namespace_id
}

output "redshift_role_arn" {
  description = "IAM role ARN attached to Redshift Serverless for S3/Glue access."
  value       = module.redshift_serverless.redshift_role_arn
}

output "sns_alert_topic_arn" {
  description = "SNS topic ARN for data quality and pipeline failure alerts."
  value       = aws_sns_topic.data_quality_alerts.arn
}
