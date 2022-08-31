output "misp_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "misp_service_name" {
  value = aws_ecs_service.fargate_service.name
}
