output "cluster_arn"               { value = aws_msk_cluster.main.arn }
output "cluster_name"              { value = aws_msk_cluster.main.cluster_name }
output "bootstrap_brokers_tls"    { value = aws_msk_cluster.main.bootstrap_brokers_tls  sensitive = true }
output "zookeeper_connect_string" { value = aws_msk_cluster.main.zookeeper_connect_string sensitive = true }
output "producer_consumer_policy_arn" { value = aws_iam_policy.msk_producer_consumer.arn }
