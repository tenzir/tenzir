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

provider "cloudflare" {
  api_token = var.cloudflare_api_token
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
}

resource "aws_security_group" "service" {
  name        = "${module.env.module_name}-${local.name}-${module.env.stage}"
  description = "Outbound access only"
  vpc_id      = var.vast_vpc_id

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
    subnets          = [var.subnet_id]
    security_groups  = [aws_security_group.service.id, var.http_app_client_security_group_id]
    assign_public_ip = true
  }

  lifecycle {
    ignore_changes = [desired_count]
  }
}


data "cloudflare_zone" "main" {
  account_id = var.cloudflare_account_id
  name       = var.cloudflare_zone
}


resource "random_id" "tunnel_secret" {
  byte_length = 32
}

resource "cloudflare_argo_tunnel" "main" {
  account_id = var.cloudflare_account_id
  name       = "${module.env.module_name}-${module.env.stage}"
  secret     = random_id.tunnel_secret.b64_std
}

resource "random_pet" "dns_pool_names" {
  count = local.dns_records
}

resource "cloudflare_record" "dns_pool" {
  count   = local.dns_records
  zone_id = data.cloudflare_zone.main.id
  name    = random_pet.dns_pool_names[count.index].id
  value   = cloudflare_argo_tunnel.main.cname
  type    = "CNAME"
  proxied = true
}
