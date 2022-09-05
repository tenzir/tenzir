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

provider "tls" {}

resource "tls_private_key" "tunneling_key" {
  algorithm = "ED25519"
}

resource "aws_cloudwatch_log_group" "fargate_logging" {
  name = "/ecs/gateway/${module.env.module_name}-${local.name}-${module.env.stage}"
}

resource "aws_efs_access_point" "mysql" {
  file_system_id = var.efs_id
  count          = var.efs_id == "" ? 0 : 1

  root_directory {
    path = "/misp/mysql"
    creation_info {
      owner_gid   = local.mysql_uid
      owner_uid   = local.mysql_gid
      permissions = "755"
    }
  }

  posix_user {
    gid = local.mysql_gid
    uid = local.mysql_uid
  }
}

resource "aws_efs_access_point" "files" {
  file_system_id = var.efs_id
  count          = var.efs_id == "" ? 0 : 1

  root_directory {
    path = "/misp/files"
    creation_info {
      owner_gid   = local.misp_gid
      owner_uid   = local.misp_uid
      permissions = "755"
    }
  }

  posix_user {
    gid = local.misp_gid
    uid = local.misp_uid
  }
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
    name = "mysql"
    dynamic "efs_volume_configuration" {
      for_each = var.efs_id == "" ? [] : [1]
      content {
        file_system_id     = var.efs_id
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = aws_efs_access_point.mysql[0].id
          iam             = "ENABLED"
        }
      }
    }
  }

  volume {
    name = "files"
    dynamic "efs_volume_configuration" {
      for_each = var.efs_id == "" ? [] : [1]
      content {
        file_system_id     = var.efs_id
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = aws_efs_access_point.files[0].id
          iam             = "ENABLED"
        }
      }
    }
  }

}

resource "aws_security_group" "service" {
  name        = "${module.env.module_name}-${local.name}-${module.env.stage}"
  description = "Allow local access"
  vpc_id      = var.vast_vpc_id

  # SSH tunnel
  ingress {
    protocol    = "tcp"
    from_port   = 2222
    to_port     = 2222
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
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
    subnets          = [var.public_subnet_id]
    security_groups  = [aws_security_group.service.id, var.efs_client_security_group_id]
    assign_public_ip = true
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}
