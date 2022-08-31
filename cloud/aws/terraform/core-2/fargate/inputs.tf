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

variable "task_cpu" {}

variable "task_memory" {}

variable "ecs_cluster_id" {}

variable "ecs_cluster_name" {}

variable "ecs_task_execution_role_arn" {}

variable "docker_image" {}

variable "entrypoint" {
  description = "The command to execute when the task starts"
  type        = string
}

variable "port" {}

variable "efs" {
  description = "Leave fields empty if you don't want to attache EFS."
  type        = object({ access_point_id = string, file_system_id = string })
  default     = { access_point_id = "", file_system_id = "" }
  validation {
    condition     = (var.efs.file_system_id == "" && var.efs.access_point_id == "") || (var.efs.file_system_id != "" && var.efs.access_point_id != "")
    error_message = "Both file_system_id and access_point_id must be empty or non-empty at the same time."
  }
}

variable "storage_mount_point" {
  description = "The path of the storage volume within the container."
}

variable "service_discov_namespace_id" {
  description = "The id of the private service discovery dns namespace."
}

variable "security_group_ids" {}

locals {
  id_raw = "${var.name}-${module.env.stage}-${var.region_name}"
  id     = substr(md5(local.id_raw), 0, 6)
}
