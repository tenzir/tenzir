include "root" {
  path = find_in_parent_folders()
}

dependency "step_1" {
  config_path = "../step-1"
}

inputs = {
  vast_lambda_image = run_cmd("bash", "-c", "VASTCLOUD_NOTTY=1 ../vast-cloud current-lambda-image --repo-arn ${dependency.step_1.outputs.vast_lambda_repository_arn}")
}
