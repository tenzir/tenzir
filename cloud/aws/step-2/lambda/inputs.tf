# global configuration

module "env" {
  source = "../env"
}

variable "region_name" {}

# function related configuration

variable "function_base_name" {}

variable "docker_image" {}

variable "memory_size" {}

variable "timeout" {}

variable "additional_policies" {
  type    = list(any)
  default = []
}

variable "environment" {
  type = map(any)
}

# VPC 

variable "vpc_id" {
  default = ""
}

variable "subnets" {
  default = []
}
