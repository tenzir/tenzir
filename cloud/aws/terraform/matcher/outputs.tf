output "matcher_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "matcher_service_name" {
  value = aws_ecs_service.fargate_service.name
}

output "matched_events_queue_url" {
  value = aws_sqs_queue.matched_events.url
}
