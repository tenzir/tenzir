#!/usr/bin/env bash

set -euo pipefail

dir=$(dirname "$(readlink -f "$0")")

tag="${1:-$(gh api \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  /repos/astral-sh/uv/releases/latest | jq -r .tag_name)}"

gen() {
  local url sha256
  url="https://github.com/astral-sh/uv/releases/download/${tag}/uv-$2.tar.gz"
  sha256="$(curl -sSL "https://github.com/astral-sh/uv/releases/download/${tag}/uv-$2.tar.gz.sha256" | cut -d' ' -f 1)"
  printf '"%s": {"url": "%s", "sha256": "%s"}' "$1" "${url}" "${sha256}"
}

cat << EOF | jq | tee "${dir}/uv-source-info.json"
{
  "version": "$tag",
  $(gen aarch64-darwin aarch64-apple-darwin),
  $(gen aarch64-linux aarch64-unknown-linux-musl),
  $(gen x86_64-darwin x86_64-apple-darwin),
  $(gen x86_64-linux x86_64-unknown-linux-musl)
}
EOF
