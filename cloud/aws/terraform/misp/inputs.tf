variable "region_name" {}
variable "misp_image" {}
variable "misp_version" {}
variable "fargate_cluster_name" {}
variable "fargate_task_execution_role_arn" {}
variable "service_discov_namespace_id" {}
variable "vast_vpc_id" {}
variable "vast_subnet_id" {}
variable "efs" {
  description = "Leave fields empty if you don't want to attache EFS."
  type        = object({ access_point_id = string, file_system_id = string })
  default     = { access_point_id = "", file_system_id = "" }
  validation {
    condition     = (var.efs.file_system_id == "" && var.efs.access_point_id == "") || (var.efs.file_system_id != "" && var.efs.access_point_id != "")
    error_message = "Both file_system_id and access_point_id must be empty or non-empty at the same time."
  }
}


module "env" {
  source = "../common/env"
}

locals {
  name          = "misp"
  mysql_version = "8.0.19"
  redis_version = "5.0.6"
  task_cpu      = 2048
  task_memory   = 4096
}
