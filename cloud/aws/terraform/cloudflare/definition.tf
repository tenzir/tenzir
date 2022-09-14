locals {
  container_definition = [
    {
      image       = "cloudflare/cloudflared"
      command     = ["tunnel", "--no-autoupdate", "run", "--token", cloudflare_argo_tunnel.main.tunnel_token]
      name        = "main"
      essential   = true
      environment = []
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.fargate_logging.name
          awslogs-region        = var.region_name
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ]
}
