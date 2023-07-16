variable "region_name" {}
variable "tenzir_lambda_role_name" {}

module "env" {
  source = "../common/env"
}
