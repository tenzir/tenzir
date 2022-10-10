output "cloudflare_task_family" {
  value = aws_ecs_task_definition.fargate_task_def.family
}

output "cloudflare_service_name" {
  value = aws_ecs_service.fargate_service.name
}

output "cloudflare_hostnames" {
  value = join(",", [for dns in cloudflare_record.dns_pool : dns.hostname])
}

output "cloudflare_tunnel_id" {
  value = cloudflare_argo_tunnel.main.id
}
