terraform {
  required_version = ">=1"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }

  }
}

# The default provider manages VAST resources and other monitoring appliances
provider "aws" {
  region = var.region_name
  default_tags {
    tags = {
      module      = module.env.module_name
      provisioner = "terraform"
      stage       = terraform.workspace
    }
  }
}

module "env" {
  source = "./env"
}
