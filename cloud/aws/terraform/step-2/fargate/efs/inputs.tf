module "env" {
  source = "../../../modules/env"
}

variable "name" {}

variable "vpc_id" {}

variable "subnet_id" {}

variable "ingress_security_group_id" {}
