#!/usr/bin/env bash

set -euo pipefail

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C "${dir}" rev-parse --show-toplevel)

get-submodule-rev() {
  local _submodule="$1"
  git -C "${toplevel}" submodule status -- "${_submodule}" | cut -c2- | cut -d' ' -f 1
}

echo "Updating contrib/tenzir-plugins"
vast_plugins_rev="$(get-submodule-rev "${toplevel}/contrib/tenzir-plugins")"
vast_plugins_json="$(jq --arg rev "${vast_plugins_rev#rev}" \
  '."rev" = $rev' "${dir}/vast/plugins/source.json")"

if git -C "${toplevel}/contrib/tenzir-plugins" merge-base --is-ancestor \
  "$(git -C "${toplevel}/contrib/tenzir-plugins" rev-parse HEAD)" \
  "$(git -C "${toplevel}/contrib/tenzir-plugins" rev-parse origin/main)"; then
  # Remove 'allRefs = true' in case it was there before.
  vast_plugins_json="$(jq 'del(.allRefs)' <<< "$vast_plugins_json")"
else
  # Insert 'allRefs = true' so Nix will find the rev that is not on the main
  # branch.
  vast_plugins_json="$(jq '."allRefs" = true' <<< "$vast_plugins_json")"
fi

echo -E "${vast_plugins_json}" > "${dir}/vast/plugins/source.json"

echo "Extracting plugin versions..."
{
  printf "[\n"
  for cm in "${toplevel}"/contrib/tenzir-plugins/*/CMakeLists.txt; do
    plugin="$(basename "$(dirname "$cm")")"
    echo "  \"$plugin\""
  done
  printf "]\n"
} > "${dir}/vast/plugins/names.nix"
