variable "region_name" {}
variable "fargate_cluster_name" {}
variable "fargate_task_execution_role_arn" {}
variable "tenzir_server_hostname" {}
variable "matcher_client_image" {}
variable "tenzir_client_security_group_id" {}
variable "tenzir_subnet_id" {}


module "env" {
  source = "../common/env"
}

locals {
  name                      = "matcher-client"
  task_cpu                  = 512
  task_memory               = 4096
  message_retention_seconds = 60
}
