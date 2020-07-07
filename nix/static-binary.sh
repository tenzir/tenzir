#!/usr/bin/env nix-shell
#!nix-shell -i bash -p git nix coreutils nix-prefetch-github

dir="$(dirname "$(readlink -f "$0")")"
toplevel="$(git -C ${dir} rev-parse --show-toplevel)"
desc="$(git -C ${dir} describe)"
vast_rev="$(git -C "${toplevel}" rev-parse HEAD)"

target="${STATIC_BINARY_TARGET:-vast}"

if [ "$1" == "--use-head" ]; then
  source_json="$(nix-prefetch-github --rev=${vast_rev} tenzir vast)"
  read -r -d '' exp <<EOF
  with import ${dir} {};
  pkgsStatic."${target}".override {
    vast-source = fetchFromGitHub (builtins.fromJSON ''${source_json}'');
    versionOverride = "${desc}";
  }
EOF
else
  read -r -d '' exp <<EOF
  with import ${dir} {};
  pkgsStatic."${target}".override {
    versionOverride = "${desc}";
  }
EOF
fi

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
