#!/usr/bin/env bash

set -euo pipefail

>&2 nix --version

log() {
  >&2 echo "$@"
}

usage() {
  printf "usage: %s [options]\n" $(basename $0)
  echo
  echo 'options:'
  echo "    -h,--help               print this message"
  echo "       --with-plugin=<path> add <path> to the list of bundled plugins"
  echo "    -D<CMake option>        options starting with \"-D\" are passed to CMake"
  echo
}

dir="$(dirname "$(readlink -f "$0")")"
toplevel="$(git -C ${dir} rev-parse --show-toplevel)"

TENZIR_BUILD_VERSION="${TENZIR_BUILD_VERSION:=$(git -C "${toplevel}" describe --abbrev=10 --long --dirty --match='v[0-9]*')}"
TENZIR_BUILD_VERSION_SHORT="${TENZIR_BUILD_VERSION_SHORT:=$(git -C "${toplevel}" describe --abbrev=10 --match='v[0-9]*')}"

desc="${TENZIR_BUILD_VERSION}"
desc_short="${TENZIR_BUILD_VERSION_SHORT}"
tenzir_rev="$(git -C "${toplevel}" rev-parse HEAD)"
log "rev is ${tenzir_rev}"

cmakeFlags=""
declare -a extraPlugins

while [ $# -ne 0 ]; do
  case "$1" in
  -D*)
    cmakeFlags="$cmakeFlags \"$1\""
    shift
    continue
    ;;
  -*=*)
    optarg="$(echo "$1" | sed 's/[-_a-zA-Z0-9]*=//')"
    ;;
  *)
    optarg=
    ;;
  esac
  case "$1" in
  --help | -h)
    usage
    exit 1
    ;;
  --with-plugin=*)
    extraPlugins+=("$(realpath "${optarg}")")
    ;;
  esac
  shift
done

plugin_version() {
  local plugin="$1"
  local name="${plugin##*/}"
  local key="TENZIR_PLUGIN_${name^^}_REVISION"
  local value="g$(git -C "${plugin}" rev-list --abbrev-commit --abbrev=10 -1 HEAD -- "${plugin}")"
  echo "-D${key}=${value}"
}

# Get Plugin versions
for plugin in "${extraPlugins[@]}"; do
  cmakeFlags="${cmakeFlags} \"$(plugin_version ${plugin})\""
done

read -r -d '' exp <<EOF || true
let pkgs = (import ${dir}).pkgs."\${builtins.currentSystem}"; in
(pkgs.pkgsStatic.tenzir.override {
  versionLongOverride = "${desc}";
  versionShortOverride = "${desc_short}";
  extraPlugins = [ ${extraPlugins[@]} ];
  extraCmakeFlags = [ ${cmakeFlags} ];
}).package
EOF

log running "nix --print-build-logs build --print-out-paths --impure --expr  \'${exp}\'"
nix --print-build-logs build --print-out-paths --impure --expr "${exp}"
