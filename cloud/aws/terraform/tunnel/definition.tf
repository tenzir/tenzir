locals {
  container_definition = [
    {
      image     = "ngrok/ngrok"
      name      = "main"
      essential = true
      entrypoint = ["sleep", "infinity"]
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
