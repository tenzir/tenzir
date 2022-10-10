include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

# Core 1 is required for image deployment
dependency "core_1" {
  config_path  = "../core-1"
  skip_outputs = true
}

retryable_errors = [
  "(?s).*error deleting Lambda ENIs for EC2 Subnet .* error waiting for Lambda ENI .* to become available for detachment: timeout while waiting for state to become 'available'.*",
]

terraform {
  before_hook "deploy_images" {
    commands = ["apply"]
    execute = ["/bin/bash", "-c", <<EOT
../../vast-cloud docker-login \
                 build-images --step=core-2 \
                 push-images --step=core-2 && \
../../vast-cloud print-image-vars --step=core-2 > images.generated.tfvars
EOT
    ]
  }

  extra_arguments "image_vars" {
    commands  = ["apply"]
    arguments = ["-var-file=${get_terragrunt_dir()}/images.generated.tfvars"]
  }

}


inputs = {
  lambda_client_image = "dummy_overriden_by_before_hook"
  vast_server_image   = "dummy_overriden_by_before_hook"
  region_name         = get_env("VAST_AWS_REGION")
  peered_vpc_id       = get_env("VAST_PEERED_VPC_ID")
  vast_cidr           = get_env("VAST_CIDR")
  vast_version        = get_env("VAST_VERSION")
  vast_storage_type   = get_env("VAST_STORAGE_TYPE")
}
