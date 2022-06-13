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

variable "vast_lambda_role_name" {
}

module "env" {
  source = "../common/env"
}

locals {
  s3_subcmd   = "aws s3 --region ${var.cloudtrail_bucket_region} cp s3://${var.cloudtrail_bucket_name}/$SRC_KEY -"
  vast_subcmd = "vast import --type=aws.cloudtrail json"
  import_cmd  = "${local.s3_subcmd} | gzip -d | jq  -c '.Records[]' | ${local.vast_subcmd}"

  # create a unique id so that this stack can be deployed multiple times
  id_raw = "${var.cloudtrail_bucket_name}-${module.env.module_name}-${module.env.stage}-${var.region_name}"
  # 6 hexa digits should be more than sufficient to avoid conflicts as this stack
  # will be deployed only a very moderate amount of times within an account
  id = substr(md5(local.id_raw), 0, 6)
}
