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
  versions = read_terragrunt_config(find_in_parent_folders("versions.hcl"))
}

generate = local.versions.generate
