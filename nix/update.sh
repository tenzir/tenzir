#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git jq nix-prefetch-github nix-prefetch-git

set -euo pipefail

dir=$(dirname "$(readlink -f "$0")")
toplevel=$(git -C "${dir}" rev-parse --show-toplevel)
mainroot="$(git -C "${dir}" rev-parse --path-format=absolute --show-toplevel)"

update-source-github() {
  local _name="$1"
  local _submodule="$2"
  local _user="$3"
  local _repo="$4"
  local _path="${toplevel}/${_submodule}"
  local _rev _version
  _rev="$(git -C "${toplevel}" submodule status -- "${_submodule}" | cut -c2- | cut -d' ' -f 1)"
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
  echo "updating ${_name} at ${_submodule} from ${_url}"

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
update-source vast-plugins "contrib/vast-plugins"
