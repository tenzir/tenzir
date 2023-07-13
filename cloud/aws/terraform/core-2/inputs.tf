variable "region_name" {
  description = "The AWS region name (eu-west-1, us-east2...) in which the stack will be deployed"
}

variable "peered_vpc_id" {
  description = "An existing VPC from which data will be collected into Tenzir"
}

variable "tenzir_cidr" {
  description = "A new subnet to host Tenzir and other monitoring appliances"
}

variable "tenzir_version" {
  description = "A Tenzir release version (vX.Y.Z), or 'latest' for the most recent commit on the main branch"
}

variable "tenzir_storage_type" {
  description = <<EOF
The storage type that should be used for tasks that might need persistence:
- ATTACHED will usually have better performances, but will be lost when the task is stopped
- EFS has a higher latency and a limited bandwidth, but persists accross task executions
  EOF

  validation {
    condition     = contains(["EFS", "ATTACHED"], var.tenzir_storage_type)
    error_message = "Allowed values for tenzir_server_storage are \"EFS\" or \"ATTACHED\"."
  }
}

variable "lambda_client_image" {
  description = "The Tenzir Lambda Docker image in ECR"
}

variable "tenzir_server_image" {
  description = "The Tenzir server Docker image in ECR"
}

locals {
  id_raw = "${module.env.module_name}-${module.env.stage}-${var.region_name}"
  id     = substr(md5(local.id_raw), 0, 6)
  # this namespace will be specific to this region
  service_discov_domain = "${local.id}.tenzir.local"
  tenzir_server_name      = "tenzir-server"
  tenzir_port             = 5158
  tenzir_server_hostname  = "${local.tenzir_server_name}.${local.service_discov_domain}:${local.tenzir_port}"

}

module "env" {
  source = "../common/env"
}
