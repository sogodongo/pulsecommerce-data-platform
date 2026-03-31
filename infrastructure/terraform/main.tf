# =============================================================================
# infrastructure/terraform/main.tf
# =============================================================================
# Root Terraform configuration — PulseCommerce Data Platform (AWS).
#
# Module composition:
#   networking        → VPC, subnets, NAT, security groups, VPC endpoints
#   s3-lakehouse      → S3 Tables (Iceberg), scripts, checkpoints, KMS keys
#   msk               → Amazon MSK 3.6 cluster, Schema Registry, Debezium IAM
#   glue              → Glue Data Catalog databases, IAM role, ELT jobs
#   flink             → Managed Flink 1.19 app, IAM role, CloudWatch alarms
#   redshift-serverless → Redshift Serverless namespace + workgroup
#
# Prerequisites:
#   terraform init -backend-config=backend.hcl
#   terraform workspace select prod
# =============================================================================

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "s3" {
    # Populated via -backend-config=backend.hcl at init time
    # bucket         = "pulsecommerce-terraform-state"
    # key            = "prod/terraform.tfstate"
    # region         = "us-east-1"
    # dynamodb_table = "pulsecommerce-terraform-locks"
    # encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.tags, {
      Environment = var.environment
    })
  }
}

# ── Data sources ──────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  name_prefix = "${var.project}-${var.environment}"
}

# ── SNS alert topic (shared across modules) ───────────────────────────────────

resource "aws_sns_topic" "data_quality_alerts" {
  name              = "${local.name_prefix}-${var.data_quality_sns_topic_name}"
  kms_master_key_id = module.s3_lakehouse.kms_key_id
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "alert_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.data_quality_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── Networking ────────────────────────────────────────────────────────────────

module "networking" {
  source = "./modules/networking"

  project              = var.project
  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  availability_zones   = var.availability_zones
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
  tags                 = var.tags
}

# ── S3 Lakehouse ──────────────────────────────────────────────────────────────

module "s3_lakehouse" {
  source = "./modules/s3-lakehouse"

  project                       = var.project
  environment                   = var.environment
  aws_region                    = var.aws_region
  aws_account_id                = local.account_id
  lakehouse_bucket_name         = var.lakehouse_bucket_name
  glue_scripts_bucket_name      = var.glue_scripts_bucket_name
  flink_checkpoints_bucket_name = var.flink_checkpoints_bucket_name
  kms_key_deletion_days         = var.kms_key_deletion_days
  tags                          = var.tags
}

# ── MSK ───────────────────────────────────────────────────────────────────────

module "msk" {
  source = "./modules/msk"

  project                    = var.project
  environment                = var.environment
  aws_region                 = var.aws_region
  vpc_id                     = module.networking.vpc_id
  private_subnet_ids         = module.networking.private_subnet_ids
  msk_security_group_id      = module.networking.msk_security_group_id
  kafka_version              = var.msk_kafka_version
  broker_instance_type       = var.msk_broker_instance_type
  broker_count               = var.msk_broker_count
  broker_volume_gb           = var.msk_broker_volume_gb
  tiered_storage_enabled     = var.msk_tiered_storage_enabled
  lakehouse_bucket_arn       = module.s3_lakehouse.lakehouse_bucket_arn
  kms_key_arn                = module.s3_lakehouse.kms_key_arn
  sns_alert_topic_arn        = aws_sns_topic.data_quality_alerts.arn
  tags                       = var.tags
}

# ── Glue ──────────────────────────────────────────────────────────────────────

module "glue" {
  source = "./modules/glue"

  project                  = var.project
  environment              = var.environment
  aws_region               = var.aws_region
  aws_account_id           = local.account_id
  glue_database_bronze     = var.glue_database_bronze
  glue_database_silver     = var.glue_database_silver
  glue_database_gold       = var.glue_database_gold
  glue_scripts_bucket      = module.s3_lakehouse.glue_scripts_bucket_name
  glue_scripts_bucket_arn  = module.s3_lakehouse.glue_scripts_bucket_arn
  lakehouse_bucket_arn     = module.s3_lakehouse.lakehouse_bucket_arn
  lakehouse_bucket_name    = var.lakehouse_bucket_name
  kms_key_arn              = module.s3_lakehouse.kms_key_arn
  worker_type              = var.glue_worker_type
  max_workers              = var.glue_max_workers
  msk_brokers              = module.msk.bootstrap_brokers_tls
  schema_registry_url      = var.schema_registry_url
  tags                     = var.tags
}

# ── Flink ─────────────────────────────────────────────────────────────────────

module "flink" {
  source = "./modules/flink"

  project                      = var.project
  environment                  = var.environment
  aws_region                   = var.aws_region
  aws_account_id               = local.account_id
  vpc_id                       = module.networking.vpc_id
  private_subnet_ids           = module.networking.private_subnet_ids
  flink_security_group_id      = module.networking.flink_security_group_id
  flink_checkpoints_bucket     = module.s3_lakehouse.flink_checkpoints_bucket_name
  flink_checkpoints_bucket_arn = module.s3_lakehouse.flink_checkpoints_bucket_arn
  lakehouse_bucket_arn         = module.s3_lakehouse.lakehouse_bucket_arn
  lakehouse_bucket_name        = var.lakehouse_bucket_name
  kms_key_arn                  = module.s3_lakehouse.kms_key_arn
  msk_brokers                  = module.msk.bootstrap_brokers_tls
  msk_cluster_arn              = module.msk.cluster_arn
  schema_registry_url          = var.schema_registry_url
  parallelism                  = var.flink_parallelism
  checkpoint_interval_ms       = var.flink_checkpoint_interval_ms
  sns_alert_topic_arn          = aws_sns_topic.data_quality_alerts.arn
  tags                         = var.tags
}

# ── Redshift Serverless ───────────────────────────────────────────────────────

module "redshift_serverless" {
  source = "./modules/redshift-serverless"

  project                = var.project
  environment            = var.environment
  aws_region             = var.aws_region
  aws_account_id         = local.account_id
  vpc_id                 = module.networking.vpc_id
  private_subnet_ids     = module.networking.private_subnet_ids
  redshift_sg_id         = module.networking.redshift_security_group_id
  workgroup_name         = var.redshift_workgroup_name
  namespace_name         = var.redshift_namespace_name
  base_capacity_rpu      = var.redshift_base_capacity_rpu
  max_capacity_rpu       = var.redshift_max_capacity_rpu
  admin_username         = var.redshift_admin_username
  admin_password         = var.redshift_admin_password
  database_name          = var.redshift_database_name
  lakehouse_bucket_arn   = module.s3_lakehouse.lakehouse_bucket_arn
  lakehouse_bucket_name  = var.lakehouse_bucket_name
  kms_key_arn            = module.s3_lakehouse.kms_key_arn
  tags                   = var.tags
}
