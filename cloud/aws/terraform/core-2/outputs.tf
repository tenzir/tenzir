output "vast_task_family" {
  value = module.vast_server.task_family
}

output "vast_service_name" {
  value = module.vast_server.vast_service_name
}

output "fargate_cluster_name" {
  value = aws_ecs_cluster.fargate_cluster.name
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
