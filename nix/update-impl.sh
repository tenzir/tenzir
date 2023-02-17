#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git jq nix-prefetch-github
#!nix-shell -I nixpkgs=https://github.com/NixOS/nixpkgs/archive/a592a97fcedae7a06b8506623b25fd38a032ad13.tar.gz

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
  local _version="${3:-}"
  local _user="$4"
  local _repo="$5"
  local _path="${toplevel}/${_submodule}"
  local _rev
  _rev="$(get-submodule-rev "${_submodule}")"
  if [[ "${_version}" == "" ]]; then
    git -C "${toplevel}" submodule update --init "${_submodule}"
    _version="$(git -C "${_path}" describe --tag)"
  fi

  nix-prefetch-github --no-fetch-submodules --rev="${_rev}" "${_user}" "${_repo}" \
    | jq --arg version "${_version}" '. + {$version}' \
    > "${dir}/${_name}/source.json"
}

update-source() {
  local _name="$1"
  local _submodule="$2"
  local _version="${3:-}"
  local _url
  _url="$(git config --file "${mainroot}/.gitmodules" --get "submodule.${_submodule}.url")"
  echo "checking ${_name} at ${_submodule} from ${_url}"

  re="^((https?|ssh|git|ftps?):\/\/)?(([^\/@]+)@)?([^\/:]+)[\/:]([^\/:]+)\/(.+)\/?$"

  if [[ "${_url}" =~ ${re} ]]; then
    local _hostname=${BASH_REMATCH[5]}
    local _user=${BASH_REMATCH[6]}
    local _repo=${BASH_REMATCH[7]}

    if [[ "${_hostname}" == "github.com" ]]; then
      update-source-github "${_name}" "${_submodule}" "${_version}" "${_user}" "${_repo}"
    else
      >&2 echo "Updating submodules from ${_hostname} is not implemented"
      exit 1
    fi
  else
    >&2 echo "Remote URLs of type ${_url} not implemented"
    exit 1
  fi
}

# CAF isn't tagged properly, so we pass in the version explictly to get a
# correct derivation output path.
update-source caf "libvast/aux/caf" "0.18.7"
update-source fast_float "libvast/aux/fast_float"
