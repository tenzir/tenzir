# use the workspace name as stage
output "stage" {
  value = terraform.workspace
}

output "module_name" {
  value = "vast-cloud"
}

output "vast_server_image" {
  value = "tenzir/vast"
}

output "vast_lambda_image" {
  value = "tenzir/vast-lambda"
}
