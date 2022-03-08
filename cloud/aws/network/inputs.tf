module "env" {
  source = "../env"
}

variable "peered_vpc_id" {}

variable "subnet_cidr" {}


// split the cidr in half into a public and a private one
locals {
  private_subnet_cidr = cidrsubnet(var.subnet_cidr, 1, 0)
  public_subnet_cidr  = cidrsubnet(var.subnet_cidr, 1, 1)
}
