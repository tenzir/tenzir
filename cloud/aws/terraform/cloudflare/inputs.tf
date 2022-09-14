variable "region_name" {}
variable "cloudflare_account_id" {}
variable "cloudflare_api_token" {}
variable "cloudflare_zone" {}
variable "fargate_cluster_name" {}
variable "fargate_task_execution_role_arn" {}
variable "http_app_client_security_group_id" {}
variable "vast_vpc_id" {}
variable "subnet_id" {}

module "env" {
  source = "../common/env"
}

locals {
  name        = "cloudflare"
  dns_records = 3
  task_cpu    = 512
  task_memory = 1024
}
