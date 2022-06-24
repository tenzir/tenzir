variable "region_name" {
}

variable "source_name" {
}

variable "source_bucket_name" {
}

variable "source_bucket_region" {
}

variable "vast_lambda_name" {
}

variable "vast_lambda_arn" {
}

variable "vast_lambda_role_name" {
}

variable "import_cmd" {
  description = "The bash command to execute. The object key is identified by the environement variables SRC_KEY."
}

module "env" {
  source = "../env"
}

locals {
  id_raw = "${var.source_name}-${var.source_bucket_name}-${module.env.module_name}-${module.env.stage}-${var.region_name}"
  id     = substr(md5(local.id_raw), 0, 6)
}
