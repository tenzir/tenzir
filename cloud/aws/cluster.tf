resource "aws_subnet" "ids" {
  vpc_id     = var.vpc_id
  cidr_block = var.subnet_cidr

  tags = module.env.default_tags
}

resource "aws_ecs_cluster" "fargate_cluster" {
  name               = "${module.env.module_name}-cluster-${module.env.stage}"
  capacity_providers = ["FARGATE"]
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
  }
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
  tags = module.env.default_tags
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

  tags = module.env.default_tags
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
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "ecs:StartTelemetrySession"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}
