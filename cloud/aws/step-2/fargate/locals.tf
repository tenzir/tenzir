locals {
  container_definition = [
    {
      cpu       = var.task_cpu
      image     = var.docker_image
      memory    = var.task_memory
      name      = "main"
      essential = true
      mountPoints = [
        {
          sourceVolume  = "storage",
          containerPath = var.storage_mount_point,
          readOnly      = false
        }
      ]
      entrypoint = ["/bin/sh", "-c"]
      command    = ["${var.entrypoint}"]
      portMappings = [
        {
          containerPort = var.port
          hostPort      = var.port
          protocol      = "tcp"
        },
      ]
      volumesFrom = []
      environment = var.environment
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
