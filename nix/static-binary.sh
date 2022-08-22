#!/usr/bin/env bash

nix --version
nix-prefetch-github --version workaround bug

log () {
  >&2 echo "$@"
}

usage() {
  printf "usage: %s [options]\n" $(basename $0)
  echo
  echo 'options:'
  echo "    -h,--help               print this message"
  echo "       --use-head           build from the latest commit instead of your current"
  echo "                            working copy"
  echo "       --with-plugin=<path> add <path> to the list of bundled plugins (pcap and"
  echo "                            broker are enabled automatically)"
  echo "    -D<CMake option>        options starting with "-D" are passed to CMake"
  echo
}

dir="$(dirname "$(readlink -f "$0")")"
toplevel="$(git -C ${dir} rev-parse --show-toplevel)"

VAST_BUILD_VERSION="${VAST_BUILD_VERSION:=$(git -C "${toplevel}" describe --abbrev=10 --long --dirty --match='v[0-9]*')}"
VAST_BUILD_VERSION_SHORT="${VAST_BUILD_VERSION_SHORT:=$(git -C "${toplevel}" describe --abbrev=10 --match='v[0-9]*')}"

desc="${VAST_BUILD_VERSION}"
artifact_name="${VAST_BUILD_VERSION_SHORT}"
vast_rev="$(git -C "${toplevel}" rev-parse HEAD)"
log "rev is ${vast_rev}"

target="${STATIC_BINARY_TARGET:-vast}"

USE_HEAD="off"
cmakeFlags=""
# Enable the bundled plugins by default.
plugins=(
  "${toplevel}/plugins/broker"
  "${toplevel}/plugins/parquet"
  "${toplevel}/plugins/pcap"
  "${toplevel}/plugins/sigma"
)

while [ $# -ne 0 ]; do
  case "$1" in
    -D*)
      cmakeFlags="$cmakeFlags \"$1\""
      shift
      continue;
      ;;
    -*=*)
      optarg="$(echo "$1" | sed 's/[-_a-zA-Z0-9]*=//')"
      ;;
    *)
      optarg=
      ;;
  esac
  case "$1" in
    --help|-h)
      usage
      exit 1
      ;;
    --use-head)
      USE_HEAD="on"
      ;;
    --with-plugin=*)
      plugins+=("$(realpath "${optarg}")")
      ;;
  esac
  shift
done

plugin_version() {
  local plugin="$1"
  local name="${plugin##*/}"
  local key="VAST_PLUGIN_${name^^}_REVISION"
  local value="g$(git -C "${plugin}" rev-list --abbrev-commit --abbrev=10 -1 HEAD -- "${plugin}")"
  echo "-D${key}=${value}"
}

# Get Plugin versions
for plugin in "${plugins[@]}"; do
  cmakeFlags="${cmakeFlags} \"$(plugin_version ${plugin})\""
done

if [ "${USE_HEAD}" == "on" ]; then
  source_json="$(nix-prefetch-github --rev=${vast_rev} tenzir vast)"
  desc="$(git -C "${toplevel}" describe --abbrev=10 --long --dirty --match='v[0-9]*' HEAD)"
  read -r -d '' exp <<EOF
  let pkgs = (import ${dir}).pkgs."\${builtins.currentSystem}"; in
  pkgs.pkgsStatic."${target}".override {
    vast-source = pkgs.fetchFromGitHub (builtins.fromJSON ''${source_json}'');
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
    buildAsPackage = true;
  }
EOF
else
  read -r -d '' exp <<EOF
  let pkgs = (import ${dir}).pkgs."\${builtins.currentSystem}"; in
  pkgs.pkgsStatic."${target}".override {
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
    buildAsPackage = true;
  }
EOF
fi

log running "nix --print-build-logs build --print-out-paths --impure --expr  \'${exp}\'"
nix --print-build-logs build --print-out-paths --impure --expr "${exp}"
