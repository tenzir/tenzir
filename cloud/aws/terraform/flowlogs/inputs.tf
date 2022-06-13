variable "region_name" {}
variable "source_bucket_name" {}
variable "source_bucket_region" {}
variable "vast_lambda_name" {}
variable "vast_lambda_arn" {}
variable "vast_lambda_role_name" {}

locals {
  s3_subcmd   = "aws s3 --region ${var.source_bucket_region} cp s3://${var.source_bucket_name}/$SRC_KEY -"
  vast_subcmd = "vast import --type=aws.flowlogs csv"
  import_cmd  = "${local.s3_subcmd} | gzip -d | tr ' ' , | ${local.vast_subcmd}"
}

module "env" {
  source = "../common/env"
}
