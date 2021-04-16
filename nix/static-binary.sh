#!/usr/bin/env nix-shell
#!nix-shell -I nixpkgs=https://github.com/NixOS/nixpkgs/archive/449b698a0b554996ac099b4e3534514528019269.tar.gz -i bash -p git nix coreutils nix-prefetch-github

nix --version
nix-prefetch-github --version

dir="$(dirname "$(readlink -f "$0")")"
toplevel="$(git -C ${dir} rev-parse --show-toplevel)"
desc="$(git -C ${dir} describe --tags --long --abbrev=10 --dirty )"
vast_rev="$(git -C "${toplevel}" rev-parse HEAD)"
echo "rev is ${vast_rev}"

target="${STATIC_BINARY_TARGET:-vast}"

USE_HEAD="off"
cmakeFlags=""
# Enable the PCAP plugin by default.
plugins=("plugins/pcap")

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
      echo "${usage}" 1>&2
      exit 1
      ;;
    --use-head)
      USE_HEAD="on"
      ;;
    --with-plugin=*)
      plugins+=("${optarg}")
      ;;
  esac
  shift
done

if [ "${USE_HEAD}" == "on" ]; then
  source_json="$(nix-prefetch-github --rev=${vast_rev} tenzir vast)"
  desc="$(git -C ${dir} describe --tags --long --abbrev=10 HEAD)"
  read -r -d '' exp <<EOF
  with import ${dir} {};
  pkgsStatic."${target}".override {
    vast-source = fetchFromGitHub (builtins.fromJSON ''${source_json}'');
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
  }
EOF
else
  read -r -d '' exp <<EOF
  with import ${dir} {};
  pkgsStatic."${target}".override {
    versionOverride = "${desc}";
    withPlugins = [ ${plugins[@]} ];
    extraCmakeFlags = [ ${cmakeFlags} ];
  }
EOF
fi

echo running "nix-build --no-out-link -E \'${exp}\'"
result=$(nix-build --no-out-link -E "${exp}")

mkdir -p build
tar -C "${result}" \
  --exclude lib \
  --exclude include \
  --exclude nix-support \
  --exclude share/vast/test \
  --exclude share/vast/integration \
  --mode='u+w' \
  -cvzf "$PWD/build/${target}-${desc}-static.tar.gz" \
  $(ls "${result}")
