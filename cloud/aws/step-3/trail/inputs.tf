variable "bucket_name" {
}

variable "region" {
}

variable "target_bus_arn" {
}

module "env" {
  source = "../../modules/env"
}

locals {
  id = substr(md5(var.bucket_name), 0, 6)
}
