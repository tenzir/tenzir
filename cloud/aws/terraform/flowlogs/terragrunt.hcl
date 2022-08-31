include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

dependency "core_2" {
  config_path = "../core-2"

  mock_outputs = {
    vast_lambda_name      = "temporary-dummy-name"
    vast_lambda_arn       = "arn:aws:lambda:::function:temporary-dummy-arn"
    vast_lambda_role_name = "temporary-dummy-name"
  }
}

locals {
  region_name          = get_env("VAST_AWS_REGION")
  source_bucket_name   = get_env("VAST_FLOWLOGS_BUCKET_NAME", "temporary-dummy-name")
  source_bucket_region = get_env("VAST_FLOWLOGS_BUCKET_REGION", "us-east-1") # needs to be a real region to pass validation
}


terraform {
  after_hook "enable_eventbridge_notifications" {
    commands = ["apply"]
    execute  = ["../resources/scripts/bucket-notif.sh", local.source_bucket_region, local.source_bucket_name]
  }
}

inputs = {
  region_name           = local.region_name
  source_bucket_name    = local.source_bucket_name
  source_bucket_region  = local.source_bucket_region
  vast_lambda_name      = dependency.core_2.outputs.vast_lambda_name
  vast_lambda_arn       = dependency.core_2.outputs.vast_lambda_arn
  vast_lambda_role_name = dependency.core_2.outputs.vast_lambda_role_name
}
