output "vast_task_family" {
  value = module.vast_server.task_family
}

output "vast_server_service_name" {
  value = module.vast_server.service_name
}

output "fargate_cluster_name" {
  value = aws_ecs_cluster.fargate_cluster.name
}

output "fargate_task_execution_role_arn" {
  value = aws_iam_role.fargate_task_execution_role.arn
}

output "vast_vpc_id" {
  value = module.network.new_vpc_id
}

output "vast_subnet_id" {
  value = module.network.private_subnet_id
}

output "vast_client_security_group_id" {
  value = aws_security_group.vast_client.id
}

output "vast_lambda_name" {
  value = module.vast_client.lambda_name
}

output "vast_lambda_arn" {
  value = module.vast_client.lambda_arn
}

output "vast_lambda_role_name" {
  value = module.vast_client.lambda_role_name
}

output "vast_server_domain_name" {
  value = local.vast_server_domain_name
}

output "service_discov_namespace_id" {
  value = aws_service_discovery_private_dns_namespace.main.id
}
