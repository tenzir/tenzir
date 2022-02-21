output "vast_security_group" {
  value = module.vast.task_security_group_id
}

output "vast_task_definition" {
  value = module.vast.task_definition_arn
}

output "fargate_cluster_name" {
  value = aws_ecs_cluster.fargate_cluster.name
}

output "ids_subnet_id" {
  value = aws_subnet.ids.id
}
