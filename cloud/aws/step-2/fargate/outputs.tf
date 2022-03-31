output "task_definition_arn" {
  value = aws_ecs_task_definition.fargate_task_def.arn
}

output "task_security_group_id" {
  value = aws_security_group.ecs_tasks.id
}

output "task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.fargate_logging.name
}
