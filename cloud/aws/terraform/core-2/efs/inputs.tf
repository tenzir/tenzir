module "env" {
  source = "../../common/env"
}

variable "name" {}

variable "vpc_id" {}

variable "subnet_id" {}

variable "security_group_id" {}
