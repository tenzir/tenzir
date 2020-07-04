#!/usr/bin/env nix-shell
#!nix-shell -i bash -p git nix coreutils

dir="$(dirname "$(readlink -f "$0")")"
desc="$(git -C ${dir} describe)"

target="${1:-vast}"

read -r -d '' exp <<EOF
with import ${dir} {};
pkgsStatic."${target}".override {
  versionOverride = "${desc}";
}
EOF

nix-build -E "${exp}"

mkdir -p build
tar -C result \
  --exclude lib \
  --exclude include \
  --exclude nix-support \
  --exclude share/vast/test \
  --exclude share/vast/integration \
  --mode='u+w' \
  -cvzf "$PWD/build/${target}-${desc}.tar.gz" \
  $(ls result)
