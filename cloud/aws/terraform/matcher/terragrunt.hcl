include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

dependency "core_1" {
  config_path = "../core-1"

  mock_outputs = {
    vast_repository_arn = "arn:aws:ecr:::repository/temporary-dummy-arn"
  }
}

dependency "core_2" {
  config_path = "../core-2"

  mock_outputs = {
    fargate_task_execution_role_arn = "arn:aws:iam:::role/temporary-dummy-arn"
    fargate_cluster_name            = "dummy_name"
    vast_server_domain_name         = "dummy.local"
    vast_subnet_id                  = "dummy_id"
    vast_client_security_group_id   = "dummy_id"
  }
}

locals {
  region_name = get_env("VAST_AWS_REGION")
}

terraform {
  before_hook "deploy_images" {
    commands = ["apply"]
    execute  = ["../../resources/scripts/deploy-images.sh", dependency.core_1.outputs.vast_repository_arn, "matcher_client"]
  }

  extra_arguments "image_vars" {
    commands  = ["apply"]
    arguments = ["-var-file=${get_terragrunt_dir()}/images.generated.tfvars"]
  }

}

inputs = {
  region_name                     = local.region_name
  matcher_client_image            = "dummy_overriden_by_before_hook"
  fargate_task_execution_role_arn = dependency.core_2.outputs.fargate_task_execution_role_arn
  vast_server_domain_name         = dependency.core_2.outputs.vast_server_domain_name
  fargate_cluster_name            = dependency.core_2.outputs.fargate_cluster_name
  vast_subnet_id                  = dependency.core_2.outputs.vast_subnet_id
  vast_client_security_group_id   = dependency.core_2.outputs.vast_client_security_group_id
}
