#!/usr/bin/env bash

set -euo pipefail

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C "${dir}" rev-parse --show-toplevel)

get-submodule-rev() {
  local _submodule="$1"
  git -C "${toplevel}" submodule status -- "${_submodule}" | cut -c2- | cut -d' ' -f 1
}

echo "Updating contrib/vast-plugins"
vast_plugins_rev="$(get-submodule-rev "${toplevel}/contrib/vast-plugins")"
vast_plugins_json="$(jq --arg rev "${vast_plugins_rev#rev}" \
  '."rev" = $rev' "${dir}/vast/plugins/source.json")"
echo -E "${vast_plugins_json}" > "${dir}/vast/plugins/source.json"

echo "Extracting plugin versions..."
{
  printf "{\n"
  for cm in "${toplevel}"/contrib/vast-plugins/*/CMakeLists.txt; do
    plugin="$(basename "$(dirname "$cm")")"
    # Extract the plugin version from CMakeLists.txt.
    # TODO: Remove this and only store the plugin names when we no longer print
    # plugin versions in `vast version`.
    if [[ "$OSTYPE" == "darwin"* ]]; then
      SED=gsed
    else
      SED=sed
    fi
    version="$("${SED}" -n 's|[^(]VERSION\s\+\([0-9A-Za-z\.-_]\+\)$|\1|p' "$cm" | tr -d '[:space:]')"
    echo "plugin = $plugin, version = $version" >&2
    echo "  $plugin = \"$version\";"
  done
  printf "}\n"
} > "${dir}/vast/plugins/versions.nix"
