output "misp_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "misp_service_name" {
  value = aws_ecs_service.fargate_service.name
}

output "ssh_tunneling_private_key" {
  value     = tls_private_key.tunneling_key.private_key_openssh
  sensitive = true
}

output "exposed_services" {
  value = "http://${local.name}.${var.service_discov_domain}:8080"
}
