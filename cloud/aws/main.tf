terraform {
  backend "local" {
    path = ".terraform/state/terraform.tfstate"
  }
  required_version = ">=0.12"

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

provider "aws" {
  region = var.region_name
}

module "env" {
  source = "./env"
}
