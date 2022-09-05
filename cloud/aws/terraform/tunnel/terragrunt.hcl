include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

dependency "core_2" {
  config_path = "../core-2"

  mock_outputs = {
    fargate_task_execution_role_arn = "arn:aws:iam:::role/temporary-dummy-arn"
    fargate_cluster_name            = "dummy_name"
    vast_vpc_id                     = "dummy_id"
    vast_subnet_id                  = "dummy_id"
    vast_client_security_group_id   = "dummy_id"
  }
}

locals {
  region_name  = get_env("VAST_AWS_REGION")
}

# terraform {
#   before_hook "deploy_images" {
#     commands = ["apply"]
#     execute  = ["../../resources/scripts/deploy-cloudflared-image.sh"]
#   }

#   extra_arguments "image_vars" {
#     commands  = ["apply"]
#     arguments = ["-var-file=${get_terragrunt_dir()}/images.generated.tfvars"]
#   }

# }

inputs = {
  region_name                     = local.region_name
  fargate_task_execution_role_arn = dependency.core_2.outputs.fargate_task_execution_role_arn
  fargate_cluster_name            = dependency.core_2.outputs.fargate_cluster_name
  vast_vpc_id                     = dependency.core_2.outputs.vast_vpc_id
  vast_subnet_id                  = dependency.core_2.outputs.vast_subnet_id
}
