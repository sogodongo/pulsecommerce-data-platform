variable "project"                       { type = string }
variable "environment"                   { type = string }
variable "aws_region"                    { type = string }
variable "aws_account_id"               { type = string }
variable "lakehouse_bucket_name"        { type = string }
variable "glue_scripts_bucket_name"     { type = string }
variable "flink_checkpoints_bucket_name" { type = string }
variable "kms_key_deletion_days"        { type = number  default = 30 }
variable "tags"                         { type = map(string) default = {} }
