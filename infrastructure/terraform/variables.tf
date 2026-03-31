variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account ID — used to construct ARNs."
  type        = string
}

variable "environment" {
  description = "Deployment environment (prod / staging / dev)."
  type        = string
  default     = "prod"
  validation {
    condition     = contains(["prod", "staging", "dev"], var.environment)
    error_message = "environment must be prod, staging, or dev."
  }
}

variable "project" {
  description = "Project name prefix applied to all resource names and tags."
  type        = string
  default     = "pulsecommerce"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "availability_zones" {
  type    = list(string)
  default = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "private_subnet_cidrs" {
  type    = list(string)
  default = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  type    = list(string)
  default = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "msk_kafka_version" {
  type    = string
  default = "3.6.0"
}

variable "msk_broker_instance_type" {
  type    = string
  default = "kafka.m5.2xlarge"
}

variable "msk_broker_count" {
  type    = number
  default = 3
}

variable "msk_broker_volume_gb" {
  type    = number
  default = 1000
}

variable "msk_tiered_storage_enabled" {
  type    = bool
  default = true
}

variable "schema_registry_url" {
  description = "Confluent Schema Registry URL."
  type        = string
}

variable "lakehouse_bucket_name" {
  description = "S3 Tables bucket name for Bronze/Silver/Gold Iceberg data."
  type        = string
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket for Glue scripts and Spark event logs."
  type        = string
}

variable "flink_checkpoints_bucket_name" {
  description = "S3 bucket for Flink RocksDB incremental checkpoints."
  type        = string
}

variable "kms_key_deletion_days" {
  type    = number
  default = 30
}

variable "glue_database_bronze" {
  type    = string
  default = "bronze"
}

variable "glue_database_silver" {
  type    = string
  default = "silver"
}

variable "glue_database_gold" {
  type    = string
  default = "gold"
}

variable "glue_worker_type" {
  type    = string
  default = "G.2X"
}

variable "glue_max_workers" {
  type    = number
  default = 10
}

variable "flink_parallelism" {
  type    = number
  default = 4
}

variable "flink_checkpoint_interval_ms" {
  type    = number
  default = 60000
}

variable "redshift_workgroup_name" {
  type    = string
  default = "pulsecommerce"
}

variable "redshift_namespace_name" {
  type    = string
  default = "pulsecommerce-ns"
}

variable "redshift_base_capacity_rpu" {
  description = "Base RPU (128–512, multiples of 8)."
  type        = number
  default     = 128
}

variable "redshift_max_capacity_rpu" {
  type    = number
  default = 512
}

variable "redshift_admin_username" {
  type      = string
  default   = "admin"
  sensitive = true
}

variable "redshift_admin_password" {
  description = "Store in Secrets Manager — never in tfvars."
  type        = string
  sensitive   = true
}

variable "redshift_database_name" {
  type    = string
  default = "analytics"
}

variable "data_quality_sns_topic_name" {
  type    = string
  default = "pulsecommerce-data-quality-alerts"
}

variable "alert_email" {
  description = "Email address for SNS alert subscription."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default = {
    Project    = "PulseCommerce"
    ManagedBy  = "Terraform"
    CostCenter = "DataPlatform"
  }
}
