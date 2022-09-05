provider "aws" {
  region = var.region_name
  default_tags {
    tags = {
      module      = module.env.module_name
      provisioner = "terraform"
      stage       = terraform.workspace
    }
  }
}

resource "aws_cloudwatch_log_group" "fargate_logging" {
  name = "/ecs/gateway/${module.env.module_name}-${local.name}-${module.env.stage}"
}

resource "aws_ecs_task_definition" "fargate_task_def" {
  family                   = "${module.env.module_name}-${local.name}-${module.env.stage}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = local.task_cpu
  memory                   = local.task_memory
  task_role_arn            = aws_iam_role.ecs_task_role.arn
  execution_role_arn       = var.fargate_task_execution_role_arn
  container_definitions    = jsonencode(local.container_definition)
  depends_on               = [aws_cloudwatch_log_group.fargate_logging] // make sure the first task does not fail because log group is not available yet

  volume {
    name = "storage"
    dynamic "efs_volume_configuration" {
      for_each = var.efs.file_system_id == "" ? [] : [1]
      content {
        file_system_id     = var.efs.file_system_id
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = var.efs.access_point_id
          iam             = "ENABLED"
        }
      }
    }
  }

}

resource "aws_security_group" "service" {
  name        = "${module.env.module_name}-${local.name}-${module.env.stage}"
  description = "Allow outbound access only"
  vpc_id      = var.vast_vpc_id

  # TODO: make this more selective
  ingress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_service_discovery_service" "fargate_svc" {
  name = local.name

  dns_config {
    namespace_id = var.service_discov_namespace_id

    dns_records {
      ttl  = 5
      type = "A"
    }

    routing_policy = "WEIGHTED"
  }

  # This health state is updated by ECS using container level health checks
  health_check_custom_config {
    failure_threshold = 1
  }
}

resource "aws_ecs_service" "fargate_service" {
  name                               = "${module.env.module_name}-${local.name}-${module.env.stage}"
  cluster                            = var.fargate_cluster_name
  task_definition                    = aws_ecs_task_definition.fargate_task_def.arn
  desired_count                      = 0
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0
  propagate_tags                     = "SERVICE"
  enable_ecs_managed_tags            = true
  launch_type                        = "FARGATE"

  network_configuration {
    subnets          = [var.vast_subnet_id]
    security_groups  = [aws_security_group.service.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn   = aws_service_discovery_service.fargate_svc.arn
    container_name = local.misp_container_name
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}
