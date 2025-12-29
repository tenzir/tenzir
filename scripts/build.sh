#!/usr/bin/env bash

# Build a CMake target, auto-discovering the build directory.
# Usage: build.sh [target] [cmake-build-options...]
# Default target: tenzir

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
build_dir=$(find "$repo_root/build" -name CMakeCache.txt -print -quit 2>/dev/null | xargs -r dirname)

if [[ -z "$build_dir" ]]; then
  echo "error: no build directory found (run cmake first)" >&2
  exit 1
fi

target=${1:-tenzir}
shift 2>/dev/null || true

exec cmake --build "$build_dir" --target "$target" "$@"
