include "root" {
  path = find_in_parent_folders()
}

dependency "step_2" {
  config_path = "../core-2"

  mock_outputs = {
    vast_lambda_name      = "temporary-dummy-name"
    vast_lambda_arn       = "arn:aws:lambda:::function:temporary-dummy-arn"
    vast_lambda_role_name = "temporary-dummy-name"
  }
}

locals {
  cloudtrail_bucket_name   = get_env("VAST_CLOUDTRAIL_BUCKET_NAME", "temporary-dummy-name")
  cloudtrail_bucket_region = get_env("VAST_CLOUDTRAIL_BUCKET_REGION", "temporary-dummy-region")
}


terraform {
  after_hook "enable_eventbridge_notifications" {
    commands = ["apply"]
    execute  = ["./bucket-notif.bash", local.cloudtrail_bucket_region, local.cloudtrail_bucket_name]
  }
}

inputs = {
  region_name              = get_env("VAST_AWS_REGION")
  cloudtrail_bucket_name   = local.cloudtrail_bucket_name
  cloudtrail_bucket_region = local.cloudtrail_bucket_region
  vast_lambda_name         = dependency.step_2.outputs.vast_lambda_name
  vast_lambda_arn          = dependency.step_2.outputs.vast_lambda_arn
  vast_lambda_role_name    = dependency.step_2.outputs.vast_lambda_role_name
}
