include "root" {
  path = find_in_parent_folders("terragrunt.${get_env("TF_STATE_BACKEND")}.hcl")
}

dependency "core_2" {
  config_path = "../core-2"

  mock_outputs = {
    fargate_task_execution_role_arn   = "arn:aws:iam:::role/temporary-dummy-arn"
    fargate_cluster_name              = "dummy_name"
    vast_vpc_id                       = "dummy_id"
    vast_subnet_id                    = "dummy_id"
    http_app_client_security_group_id = "dummy_id"
  }
}

locals {
  region_name                  = get_env("VAST_AWS_REGION")
  cloudflare_account_id        = get_env("VAST_CLOUDFLARE_ACCOUNT_ID", "dummy_id")
  cloudflare_api_token         = get_env("VAST_CLOUDFLARE_API_TOKEN", "dummy_token")
  cloudflare_zone              = get_env("VAST_CLOUDFLARE_ZONE", "dummy.zone")
  cloudflare_target_count      = length(split(",", get_env("VAST_CLOUDFLARE_EXPOSE", "dummy.url")))
  cloudflare_authorized_emails = split(",", get_env("VAST_CLOUDFLARE_AUTHORIZED_EMAILS", "dummy@dummy.dummy"))
}

inputs = {
  region_name                       = local.region_name
  cloudflare_account_id             = local.cloudflare_account_id
  cloudflare_api_token              = local.cloudflare_api_token
  cloudflare_zone                   = local.cloudflare_zone
  cloudflare_target_count           = local.cloudflare_target_count
  cloudflare_authorized_emails      = local.cloudflare_authorized_emails
  http_app_client_security_group_id = dependency.core_2.outputs.http_app_client_security_group_id
  fargate_task_execution_role_arn   = dependency.core_2.outputs.fargate_task_execution_role_arn
  fargate_cluster_name              = dependency.core_2.outputs.fargate_cluster_name
  vast_vpc_id                       = dependency.core_2.outputs.vast_vpc_id
  subnet_id                         = dependency.core_2.outputs.vast_subnet_id
}
