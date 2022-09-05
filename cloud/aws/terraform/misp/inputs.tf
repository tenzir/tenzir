variable "region_name" {}
variable "misp_image" {}
variable "misp_proxy_image" {}
variable "misp_version" {}
variable "fargate_cluster_name" {}
variable "fargate_task_execution_role_arn" {}
variable "vast_vpc_id" {}
variable "public_subnet_id" {}
variable "efs_client_security_group_id" {}
variable "efs_id" {
  description = "Leave fields empty if you don't want to attache EFS."
  default     = ""
}


module "env" {
  source = "../common/env"
}

locals {
  name        = "misp"
  task_cpu    = 2048
  task_memory = 4096

  mysql_version = "8.0.19"
  # mysql requires its user id to match the owner of the `/var/lib/mysql` dir
  mysql_uid = 1000
  mysql_gid = 1000

  redis_version = "5.0.6"

  misp_proxy_port = 8080
  # misp requires its user id to match the owner of the `/var/www/MISP/app/` dir
  misp_uid = 1000
  misp_gid = 1000
}
