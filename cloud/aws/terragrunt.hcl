# We use Terragrunt to 
# - DRY the Terraform config 
# - Manage dependencies between modules

remote_state {
  backend = "local"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }
  config = {
    path = "../.terraform/state/${path_relative_to_include()}/terraform.tfstate"
  }
}

generate "versions" {
  path      = "versions.tf"
  if_exists = "overwrite"
  contents  = <<EOF
terraform {
  required_version = ">=1"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }

  }
}
EOF
}
