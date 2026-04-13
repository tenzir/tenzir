#!/usr/bin/env bash

# Build a CMake target, auto-discovering the build directory.
# Usage: build.sh [target] [cmake-build-options...]
# Default target: tenzir

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)

# Use BUILD_DIR env var if set, otherwise auto-discover
if [[ -n ${BUILD_DIR:-} ]]; then
  build_dir="$BUILD_DIR"
else
  # Pick the configured build dir with the most recently modified CMakeCache.txt.
  # stat(1) has incompatible syntax between GNU and BSD (macOS); pick per platform.
  if [[ "$(uname)" == "Darwin" ]]; then
    stat_mtime_fmt=(-f '%m')
  else
    stat_mtime_fmt=(--format '%Y')
  fi
  latest_cache=""
  latest_mtime=0
  while IFS= read -r -d '' f; do
    m=$(stat "${stat_mtime_fmt[@]}" "$f")
    if (( m > latest_mtime )); then
      latest_mtime=$m
      latest_cache=$f
    fi
  done < <(find "$repo_root/build" -type f -name CMakeCache.txt -print0 2>/dev/null)
  if [[ -n "$latest_cache" ]]; then
    build_dir=$(dirname "$latest_cache")
  else
    build_dir=""
  fi
fi

if [[ -z $build_dir ]]; then
  echo "error: no build directory found (run cmake first or set BUILD_DIR)" >&2
  exit 1
fi

target=${1:-tenzir}
shift 2>/dev/null || true

cmake --build "$build_dir" --target "$target" "$@"
echo "=== BUILD SUCCESS ==="
