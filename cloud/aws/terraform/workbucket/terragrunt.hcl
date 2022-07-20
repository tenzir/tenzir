include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

dependency "step_2" {
  config_path = "../core-2"

  mock_outputs = {
    vast_lambda_role_name = "temporary-dummy-name"
  }
}

locals {
  region_name = get_env("VAST_AWS_REGION")
}


inputs = {
  region_name           = local.region_name
  vast_lambda_role_name = dependency.step_2.outputs.vast_lambda_role_name
}
