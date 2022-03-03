module "env" {
  source = "../env"
}

variable "region_name" {}

variable "name" {}

variable "environment" {
  type = list(object({
    name  = string
    value = string
  }))
}

variable "vpc_id" {}

variable "task_cpu" {}

variable "task_memory" {}

variable "ecs_cluster_id" {}

variable "ecs_cluster_name" {}

variable "ecs_task_execution_role_arn" {}

variable "docker_image" {}

variable "subnets" {}

variable "command" {}

variable "port" {}
