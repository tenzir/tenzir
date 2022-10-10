locals {
  container_name = "main"
  container_definition = [
    {
      cpu          = local.task_cpu
      image        = var.matcher_client_image
      memory       = local.task_memory
      name         = local.container_name
      essential    = true
      portMappings = []
      volumesFrom  = []
      environment = [{
        name  = "VAST_ENDPOINT"
        value = var.vast_server_hostname
        }, {
        name  = "QUEUE_URL"
        value = aws_sqs_queue.matched_events.url
        }, {
        name  = "AWS_REGION"
        value = var.region_name
      }]
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
