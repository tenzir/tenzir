terraform {
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

variable "peered_vpc_id" {
  description = "An existing VPC from which data will be collected into VAST"
}

variable "vast_cidr" {
  description = "A new subnet to host VAST and other monitoring appliances"
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

# This provider will create enpoints in the existing VPC to allow the metric collection.
# It can be reconfigured to use other credentials to setup cross-account monitoring.
provider "aws" {
  alias  = "monitored_vpc"
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
