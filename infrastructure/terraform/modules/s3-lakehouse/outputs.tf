output "kms_key_arn"                   { value = aws_kms_key.lakehouse.arn }
output "kms_key_id"                    { value = aws_kms_key.lakehouse.key_id }
output "lakehouse_bucket_name"         { value = aws_s3_bucket.lakehouse.bucket }
output "lakehouse_bucket_arn"          { value = aws_s3_bucket.lakehouse.arn }
output "glue_scripts_bucket_name"      { value = aws_s3_bucket.glue_scripts.bucket }
output "glue_scripts_bucket_arn"       { value = aws_s3_bucket.glue_scripts.arn }
output "flink_checkpoints_bucket_name" { value = aws_s3_bucket.flink_checkpoints.bucket }
output "flink_checkpoints_bucket_arn"  { value = aws_s3_bucket.flink_checkpoints.arn }
