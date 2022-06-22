include "root" {
  path = find_in_parent_folders()
}

terraform {
  after_hook "deploy_images" {
    commands = ["apply"]
    execute  = ["../../vast-cloud", "docker-login", "deploy-image", "--type", "lambda", "deploy-image", "--type", "fargate"]
  }
}

inputs = {
  region_name = get_env("VAST_AWS_REGION")
}
