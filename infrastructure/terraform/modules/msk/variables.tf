variable "project"                  { type = string }
variable "environment"              { type = string }
variable "aws_region"               { type = string }
variable "vpc_id"                   { type = string }
variable "private_subnet_ids"       { type = list(string) }
variable "msk_security_group_id"    { type = string }
variable "kafka_version"            { type = string  default = "3.6.0" }
variable "broker_instance_type"     { type = string  default = "kafka.m5.2xlarge" }
variable "broker_count"             { type = number  default = 3 }
variable "broker_volume_gb"         { type = number  default = 1000 }
variable "tiered_storage_enabled"   { type = bool    default = true }
variable "lakehouse_bucket_arn"     { type = string }
variable "kms_key_arn"              { type = string }
variable "sns_alert_topic_arn"      { type = string }
variable "tags"                     { type = map(string) default = {} }
