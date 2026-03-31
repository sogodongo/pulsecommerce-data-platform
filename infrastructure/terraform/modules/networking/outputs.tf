output "vpc_id"                    { value = aws_vpc.main.id }
output "private_subnet_ids"        { value = aws_subnet.private[*].id }
output "public_subnet_ids"         { value = aws_subnet.public[*].id }
output "msk_security_group_id"     { value = aws_security_group.msk.id }
output "flink_security_group_id"   { value = aws_security_group.flink.id }
output "redshift_security_group_id" { value = aws_security_group.redshift.id }
output "s3_vpc_endpoint_id"        { value = aws_vpc_endpoint.s3.id }
