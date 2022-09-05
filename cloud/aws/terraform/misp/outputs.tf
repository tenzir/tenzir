output "misp_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "service_discovery_service_id" {
  value = aws_service_discovery_service.fargate_svc.id
}
