include "root" {
  path = find_in_parent_folders()
}

dependency "core_1" {
  config_path = "../core-1"

  mock_outputs = {
    vast_lambda_repository_arn  = "arn:aws:ecr:::repository/temporary-dummy-arn"
    vast_fargate_repository_arn = "arn:aws:ecr:::repository/temporary-dummy-arn"
  }
}

retryable_errors = [
  "(?s).*error deleting Lambda ENIs for EC2 Subnet .* error waiting for Lambda ENI .* to become available for detachment: timeout while waiting for state to become 'available'.*",
]


inputs = {
  region_name              = get_env("VAST_AWS_REGION")
  peered_vpc_id            = get_env("VAST_PEERED_VPC_ID")
  vast_cidr                = get_env("VAST_CIDR")
  vast_version             = get_env("VAST_VERSION")
  vast_server_storage_type = get_env("VAST_SERVER_STORAGE_TYPE")
  vast_lambda_image        = run_cmd("bash", "-c", "VASTCLOUD_NOTTY=1 ../../vast-cloud current-image --repo-arn ${dependency.core_1.outputs.vast_lambda_repository_arn}")
  vast_server_image        = run_cmd("bash", "-c", "VASTCLOUD_NOTTY=1 ../../vast-cloud current-image --repo-arn ${dependency.core_1.outputs.vast_fargate_repository_arn}")

}
