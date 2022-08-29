include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

# Core 1 is required for image deployment
dependency "core_1" {
  config_path = "../core-1"
}

retryable_errors = [
  "(?s).*error deleting Lambda ENIs for EC2 Subnet .* error waiting for Lambda ENI .* to become available for detachment: timeout while waiting for state to become 'available'.*",
]

terraform {
  before_hook "deploy_images" {
    commands = ["apply"]
    execute  = ["../../resources/scripts/deploy-vast-images.sh", "lambda_client", "server"]
  }

  extra_arguments "image_vars" {
    commands  = ["apply"]
    arguments = ["-var-file=${get_terragrunt_dir()}/images.generated.tfvars"]
  }

}


inputs = {
  lambda_client_image      = "dummy_overriden_by_before_hook"
  server_image             = "dummy_overriden_by_before_hook"
  region_name              = get_env("VAST_AWS_REGION")
  peered_vpc_id            = get_env("VAST_PEERED_VPC_ID")
  vast_cidr                = get_env("VAST_CIDR")
  vast_version             = get_env("VAST_VERSION")
  vast_server_storage_type = get_env("VAST_SERVER_STORAGE_TYPE")
}
