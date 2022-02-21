module "vast" {
  source = "./fargate"

  name        = "vast"
  region_name = var.region_name

  vpc_id                      = var.vpc_id
  subnets                     = [aws_subnet.ids.id]
  ecs_cluster_id              = aws_ecs_cluster.fargate_cluster.id
  ecs_cluster_name            = aws_ecs_cluster.fargate_cluster.name
  ecs_task_execution_role_arn = aws_iam_role.fargate_task_execution_role.arn

  task_cpu    = 2048
  task_memory = 4096

  docker_image = "tenzir/vast:${module.env.vast_image_version}"
  command      = ["start"]
  port         = 42000

  environment = [{
    name  = "AWS_REGION"
    value = var.region_name
  }]
}
