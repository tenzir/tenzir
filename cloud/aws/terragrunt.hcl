# We use Terragrunt to 
# - DRY the Terraform config 
# - Manage dependencies between modules

remote_state {
  backend = "local"
  generate = {
    path      = "backend.generated.tf"
    if_exists = "overwrite"
  }
  config = {
    path = "../.terraform/state/${path_relative_to_include()}/terraform.tfstate"
  }
}

locals {
  versions_file = "${path_relative_from_include()}/versions.hcl"
  versions = read_terragrunt_config(local.versions_file)
}

generate = local.versions.generate
