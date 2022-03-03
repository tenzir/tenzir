locals {
  envs = {
    default = {
      vast_image_version = "latest"
    }
    test = {
      vast_image_version = "latest"
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

output "vast_image_version" {
  value = local.current_env["vast_image_version"]
}
