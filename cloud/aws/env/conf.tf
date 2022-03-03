locals {
  envs = {
    default = {
      # todo: pin release
      vast_server_image = "tenzir/vast:latest"
      vast_lambda_image = "cloudfuse/vast-lambda:latest"
    }
    test = {
      vast_server_image = "tenzir/vast:latest"
      vast_lambda_image = "cloudfuse/vast-lambda:latest"
    }
  }
  current_env = local.envs[terraform.workspace]
  module_name = "vast"
  tags = {
    module      = local.module_name
    provisioner = "terraform"
    stage       = terraform.workspace
  }
}

output "stage" {
  value = terraform.workspace
}

output "default_tags" {
  value = local.tags
}

output "module_name" {
  value = local.module_name
}

output "vast_server_image" {
  value = local.current_env["vast_server_image"]
}

output "vast_lambda_image" {
  value = local.current_env["vast_lambda_image"]
}
