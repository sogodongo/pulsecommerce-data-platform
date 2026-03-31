output "namespace_id"        { value = aws_redshiftserverless_namespace.main.id }
output "namespace_arn"       { value = aws_redshiftserverless_namespace.main.arn }
output "workgroup_arn"       { value = aws_redshiftserverless_workgroup.main.arn }
output "workgroup_endpoint"  {
  value     = aws_redshiftserverless_workgroup.main.endpoint[0].address
  sensitive = true
}
output "redshift_role_arn"   { value = aws_iam_role.redshift.arn }
output "redshift_role_name"  { value = aws_iam_role.redshift.name }
