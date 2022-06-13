include "root" {
  path = find_in_parent_folders()
}

dependency "step_1" {
  config_path = "../step-1"

  mock_outputs = {
    vast_lambda_repository_arn = "temporary-dummy-arn"
  }
}


inputs = {
  region_name = get_env("VAST_AWS_REGION")
  peered_vpc_id = get_env("VAST_PEERED_VPC_ID")
  vast_cidr = get_env("VAST_CIDR")
  vast_version = get_env("VAST_VERSION")
  vast_server_storage_type = get_env("VAST_SERVER_STORAGE_TYPE")
  vast_lambda_image = run_cmd("bash", "-c", "VASTCLOUD_NOTTY=1 ../../vast-cloud current-lambda-image --repo-arn ${dependency.step_1.outputs.vast_lambda_repository_arn}")
}
