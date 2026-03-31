variable "project"                 { type = string }
variable "environment"             { type = string }
variable "aws_region"              { type = string }
variable "aws_account_id"          { type = string }
variable "glue_database_bronze"    { type = string }
variable "glue_database_silver"    { type = string }
variable "glue_database_gold"      { type = string }
variable "glue_scripts_bucket"     { type = string }
variable "glue_scripts_bucket_arn" { type = string }
variable "lakehouse_bucket_arn"    { type = string }
variable "lakehouse_bucket_name"   { type = string }
variable "kms_key_arn"             { type = string }
variable "worker_type"             { type = string  default = "G.2X" }
variable "max_workers"             { type = number  default = 10 }
variable "msk_brokers"             { type = string  sensitive = true }
variable "schema_registry_url"     { type = string }
variable "tags"                    { type = map(string) default = {} }
