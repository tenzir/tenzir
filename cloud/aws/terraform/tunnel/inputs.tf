variable "region_name" {}
variable "tunnel_image" {}
variable "fargate_cluster_name" {}
variable "fargate_task_execution_role_arn" {}
variable "vast_vpc_id" {}
variable "vast_subnet_id" {}


module "env" {
  source = "../common/env"
}

locals {
  name          = "tunnel"
  task_cpu      = 512
  task_memory   = 1024
}
