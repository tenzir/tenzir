#!/usr/bin/env nix-shell
#!nix-shell -i bash -p coreutils git jq nix-prefetch-github
#!nix-shell -I nixpkgs=https://github.com/NixOS/nixpkgs/archive/94035b482d181af0a0f8f77823a790b256b7c3cc.tar.gz

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
    local _upstream
    _upstream="$(curl https://api.github.com/repos/tenzir/actor-framework | jq -r .parent.html_url)"
    git -C "${_path}" remote add upstream "${_upstream}"
    git -C "${_path}" fetch --tags --all
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

update-source caf "libtenzir/aux/caf" "1.1.0-123-ga21907539"
