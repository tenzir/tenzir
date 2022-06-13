include "root" {
  path = find_in_parent_folders()
}

dependency "step_2" {
  config_path = "../step-2"
}

locals {
  cloudtrail_bucket_name   = "aws-cloudtrail-logs-462855639117-43cb709b"
  cloudtrail_bucket_region = "eu-west-1"
}

terraform {
  after_hook "enable_eventbridge_notifications" {
    commands = ["apply"]
    execute  = ["./bucket-notif.bash", local.cloudtrail_bucket_region, local.cloudtrail_bucket_name]
  }
}


inputs = {
  vast_lambda_name = dependency.step_2.outputs.vast_lambda_name
  vast_lambda_arn  = dependency.step_2.outputs.vast_lambda_arn
  cloudtrail_bucket_region = local.cloudtrail_bucket_region
  cloudtrail_bucket_name = local.cloudtrail_bucket_name
}
