output "flink_role_arn"              { value = aws_iam_role.flink.arn }
output "flink_role_name"             { value = aws_iam_role.flink.name }
output "bronze_writer_app_arn"       { value = aws_kinesisanalyticsv2_application.bronze_writer.arn }
output "session_stitcher_app_arn"    { value = aws_kinesisanalyticsv2_application.session_stitcher.arn }
output "fraud_scorer_app_arn"        { value = aws_kinesisanalyticsv2_application.fraud_scorer.arn }
output "bronze_writer_app_name"      { value = aws_kinesisanalyticsv2_application.bronze_writer.name }
output "session_stitcher_app_name"   { value = aws_kinesisanalyticsv2_application.session_stitcher.name }
output "fraud_scorer_app_name"       { value = aws_kinesisanalyticsv2_application.fraud_scorer.name }
