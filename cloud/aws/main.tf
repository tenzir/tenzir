terraform {
  backend "local" {
    path = ".terraform/state/terraform.tfstate"
  }
  required_version = ">=1"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }

  }
}

variable "region_name" {
  description = "The AWS region name (eu-west-1, us-east2...) in which the stack will be deployed"
}

variable "vpc_id" {
  description = "An existing VPC that VAST should be started in"
}

variable "subnet_cidr" {
  description = "A new subnet to create in the targeted VPC to host VAST and IDS appliances"
}

variable "vast_version" {
  description = "A VAST release version (vX.Y.Z), or 'latest' for the most recent commit on the main branch"
}

variable "vast_server_storage_type" {
  description = <<EOF
The storage type that should be used for the VAST server task:
- ATTACHED will usually have better performances, but will be lost when the task is stopped
- EFS has a higher latency and a limited bandwidth, but persists accross task executions
  EOF

  validation {
    condition     = contains(["EFS", "ATTACHED"], var.vast_server_storage_type)
    error_message = "Allowed values for vast_server_storage are \"EFS\" or \"ATTACHED\"."
  }
}

// split the cidr in half into a public and a private one
locals {
  private_subnet_cidr = cidrsubnet(var.subnet_cidr, 1, 0)
  public_subnet_cidr  = cidrsubnet(var.subnet_cidr, 1, 1)
}

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

provider "time" {}

module "env" {
  source = "./env"
}
