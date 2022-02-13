#!/usr/bin/env bash
nix --version

PRJ_ROOT=${PRJ_ROOT/nix/}
toplevel=${PRJ_ROOT/%/}
nixpkgs="(import ${toplevel}nix).pinned.\${builtins.currentSystem}"
vast_rev="$(git -C "${toplevel}" rev-parse HEAD)"
artifact_name="$(git -C "${toplevel}" describe --abbrev=10 --match='v[0-9]*')"
desc="$(git -C "${toplevel}" describe --abbrev=10 --long --match='v[0-9]*' HEAD)"

echo "rev is ${vast_rev}"

usage() {
  printf "usage: %s [options]\n" "$(basename "$0")"
  echo
  echo 'options:'
  echo "    -h,--help               print this message"
  echo "       --use-head           build from the latest commit instead of your current"
  echo "                            working copy"
  echo "       --with-plugin=<path> add <path> to the list of bundled plugins (pcap and"
  echo "                            broker are enabled automatically)"
  echo "    -D<CMake option>        options starting with -D are passed to CMake"
  echo
}

target="${STATIC_BINARY_TARGET:-vast}"

USE_HEAD="off"
cmakeFlags=""
# Enable the bundled plugins by default.
plugins=(
  "${toplevel}plugins/broker"
  "${toplevel}plugins/pcap"
)

# shellcheck disable=SC2116,SC2001
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
  local value
  value="g$(git -C "${plugin}" rev-list --abbrev-commit --abbrev=10 -1 HEAD -- "${plugin}")"
  echo "-D${key}=${value}"
}

# # Get Plugin versions
for plugin in "${plugins[@]}"; do
  cmakeFlags="${cmakeFlags} \"$(plugin_version "${plugin}")\""
done

if [ "${USE_HEAD}" == "on" ]; then
  source_json="$(nix-prefetch-github --rev="${vast_rev}" tenzir vast)"
  exp=$(
    cat <<EOF
  with ${nixpkgs}
  pkgsStatic."${target}".override {
    vast-source = fetchFromGitHub (builtins.fromJSON ''${source_json}'');
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
  }
EOF
  )
else
  exp=$(
    cat <<EOF
  with ${nixpkgs};
  pkgsStatic."${target}".override {
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
    }
EOF
  )
fi


echo running "nix-build --no-out-link -E '${exp}'"
result=$(nix-build --no-out-link -E "${exp}")

mkdir -p build
# shellcheck disable=SC2046
tar -C "${result}" \
  --exclude lib \
  --exclude include \
  --exclude nix-support \
  --exclude share/vast/test \
  --exclude share/vast/integration \
  --mode='u+w' \
  -cvzf "$PWD/build/${target}-${artifact_name}-linux-static.tar.gz" \
  $(ls "${result}")
