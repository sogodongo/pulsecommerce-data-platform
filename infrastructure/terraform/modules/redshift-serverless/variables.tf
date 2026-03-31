variable "project"               { type = string }
variable "environment"           { type = string }
variable "aws_region"            { type = string }
variable "aws_account_id"        { type = string }
variable "vpc_id"                { type = string }
variable "private_subnet_ids"    { type = list(string) }
variable "redshift_sg_id"        { type = string }
variable "workgroup_name"        { type = string }
variable "namespace_name"        { type = string }
variable "base_capacity_rpu"     { type = number  default = 128 }
variable "max_capacity_rpu"      { type = number  default = 512 }
variable "admin_username"        { type = string  sensitive = true }
variable "admin_password"        { type = string  sensitive = true }
variable "database_name"         { type = string  default = "analytics" }
variable "lakehouse_bucket_arn"  { type = string }
variable "lakehouse_bucket_name" { type = string }
variable "kms_key_arn"           { type = string }
variable "tags"                  { type = map(string) default = {} }
