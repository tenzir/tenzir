#!/usr/bin/env bash

set -euo pipefail

version=1.19.0
cargo_c_version=0.10.19
archive_sha256=479abe3e03ce11b6c6b9c4b452d9e5aa50268ba589dad26db6450d225706346e
required_rust_version=1.91.0
prefix=${1:-/usr/local}
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

curl --fail --location --silent --show-error \
  "https://github.com/VirusTotal/yara-x/archive/refs/tags/v${version}.tar.gz" \
  --output "$workdir/yara-x.tar.gz"
actual_sha256=$(openssl dgst -sha256 -r "$workdir/yara-x.tar.gz" | cut -d' ' -f1)
if [[ $actual_sha256 != "$archive_sha256" ]]; then
  echo "YARA-X archive checksum mismatch" >&2
  exit 1
fi
mkdir "$workdir/source"
tar -xzf "$workdir/yara-x.tar.gz" --strip-components=1 \
  -C "$workdir/source"
rust_version=$(rustc --version | sed -E 's/^rustc ([0-9]+\.[0-9]+\.[0-9]+).*/\1/')
if [[ "$(printf '%s\n%s\n' "$required_rust_version" "$rust_version" |
  sort -V | head -n1)" != "$required_rust_version" ]]; then
  echo "YARA-X ${version} requires Rust ${required_rust_version} or newer; found ${rust_version}" >&2
  exit 1
fi

cargo install cargo-c --version "$cargo_c_version" --locked \
  --root "$workdir/cargo-c"
(
  cd "$workdir/source"
  mkdir -p .cargo
  cargo vendor --locked "$workdir/vendor" >.cargo/config.toml
  PATH="$workdir/cargo-c/bin:$PATH" cargo cinstall \
    --package yara-x-capi \
    --release \
    --locked \
    --offline \
    --prefix "$prefix" \
    --libdir "$prefix/lib"
)
