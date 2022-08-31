locals {
  container_definition = [
    {
      image     = "cloudflare/cloudflared:latest"
      name      = "main"
      essential = true
      command = ["tunnel", "--no-autoupdate", "run", "--token", "eyJhIjoiYmMyNjllZjhjNzc4Njc0Nzg5Yzk2Mjc2YTQ1MGU0MmQiLCJ0IjoiMTJlODBjYzMtYzRmYS00ZTNiLTlmNzgtY2IyZWI3OThkZGFhIiwicyI6IllUVTFaRGhpTmpRdE5UUTJNaTAwTURBMExXSTVPRGt0TlRGaE5EUTVOamxpTURFNSJ9"]

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
