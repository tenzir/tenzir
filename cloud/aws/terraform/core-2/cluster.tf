resource "aws_ecs_cluster" "fargate_cluster" {
  name               = "${module.env.module_name}-cluster-${module.env.stage}"
  capacity_providers = ["FARGATE"]
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
  }
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
  configuration {
    execute_command_configuration {
      logging = "NONE"
    }
  }
}

resource "aws_iam_role" "fargate_task_execution_role" {
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${var.region_name}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "fargate_task_execution_policy" {
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${var.region_name}"
  role = aws_iam_role.fargate_task_execution_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "ecs:StartTelemetrySession",
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:GetAuthorizationToken"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_service_discovery_private_dns_namespace" "main" {
  name        = local.service_discov_domain
  description = "Service names for ${module.env.module_name}/${module.env.stage} in ${var.region_name}"
  vpc         = module.network.new_vpc_id
}
