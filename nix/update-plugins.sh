#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git jq nix-prefetch-github
#!nix-shell -I nixpkgs=https://github.com/NixOS/nixpkgs/archive/94035b482d181af0a0f8f77823a790b256b7c3cc.tar.gz

: "${GH_TOKEN:=$GITHUB_TOKEN}"

if [[ -z "${GH_TOKEN}" ]]; then
  echo "Either GH_TOKEN or GITHUB_TOKEN is required"
  exit 1
fi

set -euo pipefail

declare -a on_exit=()
trap '{
  for (( i = 0; i < ${#on_exit[@]}; i++ )); do
    eval "${on_exit[$i]}"
  done
}' INT TERM EXIT

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C "${dir}" rev-parse --show-toplevel)

name="tenzir-plugins.tar.gz"
url_base="https://github.com/tenzir/tenzir-plugins/archive"

get-submodule-rev() {
  local _submodule="$1"
  git -C "${toplevel}" submodule status -- "${_submodule}" | cut -c2- | cut -d' ' -f 1
}

NETRC_DIR="$(mktemp -d)"
on_exit=("rm -rf ${NETRC_DIR}" "${on_exit[@]}")

cat <<EOF > "$NETRC_DIR/netrc"
machine github.com
    password $GH_TOKEN
EOF


echo "Updating contrib/tenzir-plugins"
tenzir_plugins_rev="$(get-submodule-rev "${toplevel}/contrib/tenzir-plugins")"

nix --extra-experimental-features nix-command store prefetch-file \
  --name "${name}" \
  --json \
  --netrc-file "$NETRC_DIR/netrc" \
  "${url_base}/${tenzir_plugins_rev}.tar.gz" \
  | jq \
    --arg url "${url_base}/${tenzir_plugins_rev}.tar.gz" \
    --arg name "${name}" \
    '.name = $name | .url = $url | del(.storePath)' \
  > "${dir}/tenzir/plugins/source.json"
