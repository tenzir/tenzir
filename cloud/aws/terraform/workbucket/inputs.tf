variable "region_name" {}
variable "vast_lambda_role_name" {}

module "env" {
  source = "../common/env"
}
