module "env" {
  source = "../../modules/env"
}

variable "peered_vpc_id" {}

variable "new_vpc_cidr" {}


// split the cidr in half into a public and a private one
locals {
  private_subnet_cidr = cidrsubnet(var.new_vpc_cidr, 1, 0)
  public_subnet_cidr  = cidrsubnet(var.new_vpc_cidr, 1, 1)
}
