resource "aws_cloudwatch_log_group" "fargate_logging" {
  name = "/ecs/gateway/${module.env.module_name}-${var.name}-${module.env.stage}"
}


resource "aws_security_group" "ecs_tasks" {
  name        = "${module.env.module_name}_${var.name}_task_${module.env.stage}"
  description = "Allow local inbound access"
  vpc_id      = var.vpc_id

  ingress {
    protocol    = "tcp"
    from_port   = var.port
    to_port     = var.port
    cidr_blocks = [data.aws_subnet.selected.cidr_block]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

module "efs" {
  source                    = "./efs"
  count                     = var.storage_type == "EFS" ? 1 : 0
  name                      = var.name
  vpc_id                    = var.vpc_id
  subnet_id                 = var.subnet_id
  ingress_security_group_id = aws_security_group.ecs_tasks.id
}

resource "aws_ecs_task_definition" "fargate_task_def" {
  family                   = "${module.env.module_name}-${var.name}-${module.env.stage}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  task_role_arn            = aws_iam_role.ecs_task_role.arn
  execution_role_arn       = var.ecs_task_execution_role_arn
  container_definitions    = jsonencode(local.container_definition)
  depends_on               = [aws_cloudwatch_log_group.fargate_logging] // make sure the first task does not fail because log group is not available yet

  volume {
    name = "storage"
    dynamic "efs_volume_configuration" {
      for_each = var.storage_type == "EFS" ? [1] : []
      content {
        file_system_id     = module.efs[0].file_system_id
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = module.efs[0].access_point_id
          iam             = "ENABLED"
        }
      }
    }
  }
}

resource "aws_ecs_service" "fargate_service" {
  name                               = "${module.env.module_name}-${var.name}-${module.env.stage}"
  cluster                            = var.ecs_cluster_name
  task_definition                    = aws_ecs_task_definition.fargate_task_def.arn
  desired_count                      = 1
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0
  propagate_tags                     = "SERVICE"
  enable_execute_command             = true
  enable_ecs_managed_tags            = true
  launch_type                        = "FARGATE"

  network_configuration {
    subnets          = [var.subnet_id]
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.fargate_target.arn
    container_name   = local.container_name
    container_port   = var.port
  }

  lifecycle {
    ignore_changes = [desired_count]
  }

  depends_on = [aws_lb_listener.fargate_listener]
}
