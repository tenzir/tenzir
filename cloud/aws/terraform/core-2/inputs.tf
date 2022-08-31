variable "region_name" {
  description = "The AWS region name (eu-west-1, us-east2...) in which the stack will be deployed"
}

variable "peered_vpc_id" {
  description = "An existing VPC from which data will be collected into VAST"
}

variable "vast_cidr" {
  description = "A new subnet to host VAST and other monitoring appliances"
}

variable "vast_version" {
  description = "A VAST release version (vX.Y.Z), or 'latest' for the most recent commit on the main branch"
}

variable "vast_server_storage_type" {
  description = <<EOF
The storage type that should be used for the VAST server task:
- ATTACHED will usually have better performances, but will be lost when the task is stopped
- EFS has a higher latency and a limited bandwidth, but persists accross task executions
  EOF

  validation {
    condition     = contains(["EFS", "ATTACHED"], var.vast_server_storage_type)
    error_message = "Allowed values for vast_server_storage are \"EFS\" or \"ATTACHED\"."
  }
}

variable "lambda_client_image" {
  description = "The VAST Lambda Docker image in ECR"
}

variable "server_image" {
  description = "The VAST server Docker image in ECR"
}

locals {
  id_raw = "${module.env.module_name}-${module.env.stage}-${var.region_name}"
  id     = substr(md5(local.id_raw), 0, 6)
  # this namespace will be specific to this region
  service_namespace       = "${local.id}.vast.local"
  vast_server_domain_name = "vast-server.${local.service_namespace}"
  vast_port               = 42000
}

module "env" {
  source = "../common/env"
}
