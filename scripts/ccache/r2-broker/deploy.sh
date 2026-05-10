#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

env_file="${1:-.env}"
config_file=".wrangler.generated.toml"

if [[ ! -f "${env_file}" ]]; then
  echo "error: ${env_file} does not exist" >&2
  echo "create ${PWD}/${env_file} with the R2 broker deployment variables" >&2
  exit 1
fi

set -a
# shellcheck source=/dev/null
source "${env_file}"
set +a

get_var() {
  local name="$1"
  local fallback="${2:-}"
  local value="${!name:-}"
  if [[ -n "${value}" ]]; then
    printf '%s' "${value}"
  else
    printf '%s' "${fallback}"
  fi
}

require_var() {
  local name="$1"
  local value
  value="$(get_var "${name}")"
  if [[ -z "${value}" ]]; then
    echo "error: ${name} is required in ${env_file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

toml_string() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

cloudflare_account_id="$(get_var TENZIR_CLOUDFLARE_ACCOUNT_ID "$(get_var CLOUDFLARE_ACCOUNT_ID)")"
if [[ -z "${cloudflare_account_id}" ]]; then
  echo "error: TENZIR_CLOUDFLARE_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID is required in ${env_file}" >&2
  exit 1
fi

deploy_token="$(get_var CLOUDFLARE_WORKERS_API_TOKEN "$(get_var CLOUDFLARE_API_TOKEN)")"
if [[ -z "${deploy_token}" ]]; then
  echo "error: CLOUDFLARE_WORKERS_API_TOKEN is required in ${env_file}" >&2
  echo "       CLOUDFLARE_API_TOKEN is accepted as a local alias for this token" >&2
  exit 1
fi

r2_api_token="$(require_var CCACHE_R2_API_TOKEN)"
r2_parent_access_key_id="$(
  get_var CCACHE_R2_PARENT_ACCESS_KEY_ID "$(get_var R2_PARENT_ACCESS_KEY_ID)"
)"
if [[ -z "${r2_parent_access_key_id}" ]]; then
  echo "error: CCACHE_R2_PARENT_ACCESS_KEY_ID or R2_PARENT_ACCESS_KEY_ID is required in ${env_file}" >&2
  exit 1
fi

r2_bucket="$(get_var CCACHE_R2_BUCKET "$(get_var R2_BUCKET)")"
if [[ -z "${r2_bucket}" ]]; then
  echo "error: CCACHE_R2_BUCKET or R2_BUCKET is required in ${env_file}" >&2
  exit 1
fi

github_repository="$(require_var GITHUB_REPOSITORY)"
github_subject="$(get_var GITHUB_SUBJECT "$(get_var CCACHE_R2_GITHUB_SUBJECT)")"
github_oidc_audience="$(get_var GITHUB_OIDC_AUDIENCE "$(get_var CCACHE_R2_OIDC_AUDIENCE ccache-r2-broker)")"
r2_allowed_prefixes="$(get_var R2_ALLOWED_PREFIXES "$(get_var CCACHE_R2_ALLOWED_PREFIXES ccache/)")"
r2_temp_credential_permission="$(
  get_var R2_TEMP_CREDENTIAL_PERMISSION \
    "$(get_var CCACHE_R2_TEMP_CREDENTIAL_PERMISSION object-read-write)"
)"
r2_temp_credential_ttl_seconds="$(
  get_var R2_TEMP_CREDENTIAL_TTL_SECONDS \
    "$(get_var CCACHE_R2_TEMP_CREDENTIAL_TTL_SECONDS 3600)"
)"

{
  echo 'name = "tenzir-ccache-r2-broker"'
  echo 'main = "src/index.js"'
  echo 'compatibility_date = "2026-04-30"'
  echo 'workers_dev = true'
  echo 'preview_urls = false'
  echo
  echo '[vars]'
  printf 'CLOUDFLARE_ACCOUNT_ID = %s\n' "$(toml_string "${cloudflare_account_id}")"
  printf 'R2_BUCKET = %s\n' "$(toml_string "${r2_bucket}")"
  printf 'GITHUB_REPOSITORY = %s\n' "$(toml_string "${github_repository}")"
  printf 'GITHUB_SUBJECT = %s\n' "$(toml_string "${github_subject}")"
  printf 'GITHUB_OIDC_AUDIENCE = %s\n' "$(toml_string "${github_oidc_audience}")"
  printf 'R2_ALLOWED_PREFIXES = %s\n' "$(toml_string "${r2_allowed_prefixes}")"
  printf 'R2_TEMP_CREDENTIAL_PERMISSION = %s\n' "$(toml_string "${r2_temp_credential_permission}")"
  printf 'R2_TEMP_CREDENTIAL_TTL_SECONDS = %s\n' "$(toml_string "${r2_temp_credential_ttl_seconds}")"
} >"${config_file}"

export CLOUDFLARE_API_TOKEN="${deploy_token}"
export CLOUDFLARE_ACCOUNT_ID="${cloudflare_account_id}"

printf '%s' "${r2_parent_access_key_id}" |
  wrangler secret put R2_PARENT_ACCESS_KEY_ID --config "${config_file}"

printf '%s' "${r2_api_token}" |
  wrangler secret put CLOUDFLARE_API_TOKEN --config "${config_file}"

wrangler deploy --config "${config_file}"
