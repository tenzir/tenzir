include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

inputs = {
  region_name = get_env("VAST_AWS_REGION")
}
