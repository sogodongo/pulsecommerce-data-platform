variable "project"                       { type = string }
variable "environment"                   { type = string }
variable "aws_region"                    { type = string }
variable "aws_account_id"                { type = string }
variable "vpc_id"                        { type = string }
variable "private_subnet_ids"            { type = list(string) }
variable "flink_security_group_id"       { type = string }
variable "flink_checkpoints_bucket"      { type = string }
variable "flink_checkpoints_bucket_arn"  { type = string }
variable "lakehouse_bucket_arn"          { type = string }
variable "lakehouse_bucket_name"         { type = string }
variable "kms_key_arn"                   { type = string }
variable "msk_brokers"                   { type = string  sensitive = true }
variable "msk_cluster_arn"               { type = string }
variable "schema_registry_url"           { type = string }
variable "parallelism"                   { type = number  default = 4 }
variable "checkpoint_interval_ms"        { type = number  default = 60000 }
variable "sns_alert_topic_arn"           { type = string }
variable "tags"                          { type = map(string) default = {} }
