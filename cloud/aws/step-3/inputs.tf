variable "region_name" {
}

variable "cloudtrail_bucket_name" {
}

variable "cloudtrail_bucket_region" {
}

variable "vast_lambda_name" {
}

variable "vast_lambda_arn" {
}

module "env" {
  source = "../modules/env"
}
