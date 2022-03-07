module "vast_server" {
  source = "./fargate"

  name        = "vast-server"
  region_name = var.region_name

  vpc_id                      = var.vpc_id
  ingress_subnet_cidrs        = [local.private_subnet_cidr]
  ecs_cluster_id              = aws_ecs_cluster.fargate_cluster.id
  ecs_cluster_name            = aws_ecs_cluster.fargate_cluster.name
  ecs_task_execution_role_arn = aws_iam_role.fargate_task_execution_role.arn

  task_cpu    = 2048
  task_memory = 4096

  docker_image = "${module.env.vast_server_image}:${var.vast_version}"
  command      = ["-e", "0.0.0.0:42000", "start"]
  port         = 42000

  environment = [{
    name  = "AWS_REGION"
    value = var.region_name
  }]
}

module "vast_client" {
  source = "./lambda"

  function_base_name = "client"
  region_name        = var.region_name
  docker_image       = "${aws_ecr_repository.lambda.repository_url}:${time_static.last_image_upload.unix}"
  memory_size        = 2048
  timeout            = 300

  in_vpc  = true
  vpc_id  = var.vpc_id
  subnets = [aws_subnet.ids_appliances.id]

  additional_policies = []
  environment         = {}

  depends_on = [
    null_resource.lambda_image_push
  ]
}
