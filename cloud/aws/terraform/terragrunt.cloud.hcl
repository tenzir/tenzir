generate "backend" {
  path = "backend.generated.tf"
  if_exists = "overwrite"
  contents = <<EOC
terraform {
  cloud {
    organization = "${get_env("TF_ORGANIZATION")}"
    token        = "${get_env("TF_API_TOKEN")}"
    workspaces {
      name = "${get_env("TF_WORKSPACE_PREFIX")}-${path_relative_to_include()}"
    }
  }
}
EOC
}

locals {
  versions_file = "${path_relative_from_include()}/versions.hcl"
  versions      = read_terragrunt_config(local.versions_file)
}

generate = local.versions.generate
