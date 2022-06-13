include "root" {
  path = find_in_parent_folders()
}

terraform {
  after_hook "deploy_lambda_image" {
    commands = ["apply"]
    execute  = ["../../vast-cloud", "docker-login", "deploy-lambda-image"]
  }
}

inputs = {
  region_name = get_env("VAST_AWS_REGION")
}
