module "env" {
  source = "../../common/env"
}

variable "region_name" {}

variable "name" {
  validation {
    condition     = can(regex("^[a-z\\-0-9]+$", var.name))
    error_message = "Must be a valid domain prefix."
  }
}

variable "environment" {
  type = list(object({
    name  = string
    value = string
  }))
}

variable "vpc_id" {}

variable "subnet_id" {
  description = "Resources will only accept traffic from within this subnet"
}

data "aws_subnet" "selected" {
  id = var.subnet_id
}

variable "task_cpu" {}

variable "task_memory" {}

variable "ecs_cluster_id" {}

variable "ecs_cluster_name" {}

variable "ecs_task_execution_role_arn" {}

variable "docker_image" {}

variable "entrypoint" {
  type = string
}

variable "port" {}


variable "efs_access_point_id" {
  description = "Leave empty if you don't want to attache EFS."
  default     = ""
}

variable "elastic_file_system_id" {
  description = "Leave empty if you don't want to attache EFS."
  default     = ""
}

variable "storage_mount_point" {
  description = "The path of the storage volume within the container."
}

variable "service_discov_namespace_id" {
  description = "The id of the private service discovery dns namespace."
}

variable "security_group_id" {}

locals {
  id_raw = "${var.name}-${module.env.stage}-${var.region_name}"
  id     = substr(md5(local.id_raw), 0, 6)
}
