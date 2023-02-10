#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git jq nix-prefetch-github nix-prefetch-git

set -euo pipefail

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C "${dir}" rev-parse --show-toplevel)
mainroot="$(git -C "${dir}" rev-parse --path-format=absolute --show-toplevel)"

get-submodule-rev() {
  local _submodule="$1"
  git -C "${toplevel}" submodule status -- "${_submodule}" | cut -c2- | cut -d' ' -f 1
}

update-source-github() {
  local _name="$1"
  local _submodule="$2"
  local _user="$3"
  local _repo="$4"
  local _path="${toplevel}/${_submodule}"
  local _rev _version
  _rev="$(get-submodule-rev "${_submodule}")"
  git -C "${toplevel}" submodule update --init "${_submodule}"
  _version="$(git -C "${_path}" describe --tag)"

  nix-prefetch-github --no-fetch-submodules --rev="${_rev}" "${_user}" "${_repo}" \
    | jq --arg version "${_version}" '. + {$version}' \
    > "${dir}/${_name}/source.json"
}

update-source() {
  local _name="$1"
  local _submodule="$2"
  local _url
  _url="$(git config --file "${mainroot}/.gitmodules" --get "submodule.${_submodule}.url")"
  echo "checking ${_name} at ${_submodule} from ${_url}"

  re="^((https?|ssh|git|ftps?):\/\/)?(([^\/@]+)@)?([^\/:]+)[\/:]([^\/:]+)\/(.+)\/?$"

  if [[ "${_url}" =~ ${re} ]]; then
    local _hostname=${BASH_REMATCH[5]}
    local _user=${BASH_REMATCH[6]}
    local _repo=${BASH_REMATCH[7]}

    if [[ "${_hostname}" == "github.com" ]]; then
      update-source-github "${_name}" "${_submodule}" "${_user}" "${_repo}"
    else
      >&2 echo "Updating submodules from ${_hostname} is not implemented"
      exit 1
    fi
  else
    >&2 echo "Remote URLs of type ${_url} not implemented"
    exit 1
  fi
}

update-source caf "libvast/aux/caf"
update-source fast_float "libvast/aux/fast_float"

echo "Updating contrib/vast-plugins"
vast_plugins_rev="$(get-submodule-rev "contrib/vast-plugins")"
vast_plugins_json="$(jq --arg rev "${vast_plugins_rev#rev}" \
  '."rev" = $rev' "${dir}/vast/plugins/source.json")"
echo -E "${vast_plugins_json}" > "${dir}/vast/plugins/source.json"

echo "Extracting plugin versions..."
{
  printf "{\n"
  for cm in contrib/vast-plugins/*/CMakeLists.txt; do
    plugin="$(basename "$(dirname "$cm")")"
    version="$(sed -n 's|[^(]VERSION\s\+\([0-9A-Za-z\.-_]\+\)$|\1|p' "$cm" | tr -d '[:space:]')"
    echo "plugin = $plugin, version = $version" >&2
    echo "  $plugin = \"$version\";"
  done
  printf "}\n"
} > "${dir}/vast/plugins/versions.nix"
