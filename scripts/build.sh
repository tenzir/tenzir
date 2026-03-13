#!/usr/bin/env bash

# Build a CMake target, auto-discovering the build directory.
# Usage: build.sh [target] [cmake-build-options...]
# Default target: tenzir

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
build_dir="${BUILD_DIR:-build/clang/release}"

if [[ ! -d "$build_dir" ]]; then
  echo "error: build directory '$build_dir' not found (run cmake first or set BUILD_DIR)" >&2
  exit 1
fi

target="${1:-tenzir}"
shift || true

cmake --build "$build_dir" --target "$target" "$@"
echo "=== BUILD SUCCESS ==="
