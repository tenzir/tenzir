#!/usr/bin/env bash

# Build a CMake target, auto-discovering the build directory.
# Usage: build.sh [target] [cmake-build-options...]
# Default target: tenzir

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)

# Use BUILD_DIR env var if set, otherwise auto-discover
if [[ -n "${BUILD_DIR:-}" ]]; then
  build_dir="$BUILD_DIR"
else
  build_dir=$(find "$repo_root/build" -name CMakeCache.txt -print -quit 2>/dev/null | xargs -r dirname)
fi

if [[ -z "$build_dir" ]]; then
  echo "error: no build directory found (run cmake first or set BUILD_DIR)" >&2
  exit 1
fi

target=${1:-tenzir}
shift 2>/dev/null || true

exec cmake --build "$build_dir" --target "$target" "$@"
