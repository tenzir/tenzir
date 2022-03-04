resource "aws_cloudwatch_log_group" "fargate_logging" {
  name = "/ecs/gateway/${module.env.module_name}-${var.name}-${module.env.stage}"
}


resource "aws_security_group" "ecs_tasks" {
  name        = "${module.env.module_name}_${var.name}_task_${module.env.stage}"
  description = "allow inbound access from the cidr blocks specified in the ingress_subnets only"
  vpc_id      = var.vpc_id

  ingress {
    protocol    = "tcp"
    from_port   = var.port
    to_port     = var.port
    cidr_blocks = var.ingress_subnet_cidrs
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "efs" {
  name        = "${module.env.module_name}_${var.name}_efs_${module.env.stage}"
  description = "allow inbound nfs access"
  vpc_id      = var.vpc_id

  ingress {
    protocol        = "tcp"
    from_port       = 2049
    to_port         = 2049
    security_groups = [aws_security_group.ecs_tasks.id]
  }

}

resource "aws_efs_file_system" "efs_volume" {
  count = var.storage_type == "EFS" ? 1 : 0
  tags = {
    Name = "${module.env.module_name}-${var.name}-${module.env.stage}"
  }
}

resource "aws_efs_mount_target" "efs_target" {
  count           = var.storage_type == "EFS" ? 1 : 0
  file_system_id  = aws_efs_file_system.efs_volume[0].id
  subnet_id       = var.subnet_id
  security_groups = [aws_security_group.efs.id]
}


resource "aws_efs_access_point" "access_point_for_fargate" {
  count          = var.storage_type == "EFS" ? 1 : 0
  file_system_id = aws_efs_file_system.efs_volume[0].id

  root_directory {
    path = "/storage"
    creation_info {
      owner_gid   = 1000
      owner_uid   = 1000
      permissions = "755"
    }
  }

  posix_user {
    gid = 1000
    uid = 1000
  }
}

resource "aws_ecs_task_definition" "fargate_task_def" {
  family                   = "${module.env.module_name}-${var.name}-${module.env.stage}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  task_role_arn            = aws_iam_role.ecs_task_role.arn  // necessary to access other aws services
  execution_role_arn       = var.ecs_task_execution_role_arn // necessary to log and access ecr
  container_definitions    = jsonencode(local.container_definition)
  depends_on               = [aws_cloudwatch_log_group.fargate_logging] // make sure the first task does not fail because log group is not available yet

  volume {
    name = "storage"
    dynamic "efs_volume_configuration" {
      for_each = var.storage_type == "EFS" ? [1] : []
      content {
        file_system_id     = aws_efs_file_system.efs_volume[0].id
        root_directory     = "/storage"
        transit_encryption = "ENABLED"
        authorization_config {
          access_point_id = aws_efs_access_point.access_point_for_fargate[0].id
          iam             = "ENABLED"
        }
      }
    }
  }
}
