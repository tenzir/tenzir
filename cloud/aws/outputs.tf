output "vast_vpc_id" {
  value = module.network.new_vpc_id
}

output "vast_security_group" {
  value = module.vast_server.task_security_group_id
}

output "vast_task_definition" {
  value = module.vast_server.task_definition_arn
}

output "vast_task_family" {
  value = module.vast_server.task_family
}

output "fargate_cluster_name" {
  value = aws_ecs_cluster.fargate_cluster.name
}

output "ids_appliances_subnet_id" {
  value = module.network.private_subnet_id
}

output "vast_lambda_name" {
  value = module.vast_client.lambda_name
}
