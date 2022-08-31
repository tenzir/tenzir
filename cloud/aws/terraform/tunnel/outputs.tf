output "tunnel_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "tunnel_service_name" {
  value = aws_ecs_service.fargate_service.name
}
