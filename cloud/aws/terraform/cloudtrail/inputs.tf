variable "region_name" {}
variable "source_bucket_name" {}
variable "source_bucket_region" {}
variable "tenzir_lambda_name" {}
variable "tenzir_lambda_arn" {}
variable "tenzir_lambda_role_name" {}

locals {
  s3_subcmd     = "aws s3 --region ${var.source_bucket_region} cp s3://${var.source_bucket_name}/$SRC_KEY -"
  tenzir_subcmd = "tenzir import --type=aws.cloudtrail json"
  import_cmd    = "${local.s3_subcmd} | gzip -d | jq  -c '.Records[]' | ${local.tenzir_subcmd}"
}

module "env" {
  source = "../common/env"
}
