#!/usr/bin/env bash

# Find the build directory by locating CMakeCache.txt
BUILD_DIR=$(find . -name CMakeCache.txt -type f 2>/dev/null | head -1 | xargs dirname 2>/dev/null)

if [ -n "$BUILD_DIR" ]; then
  ABS_BUILD_DIR=$(cd "$BUILD_DIR" && pwd)

  # Export environment variables for Claude Code session
  if [ -n "$CLAUDE_ENV_FILE" ]; then
    cat >>"$CLAUDE_ENV_FILE" <<EOF
export BUILD_DIR="$ABS_BUILD_DIR"
export TENZIR_BINARY="$ABS_BUILD_DIR/bin/tenzir"
export TENZIR_NODE_BINARY="$ABS_BUILD_DIR/bin/tenzir-node"
export PATH="$ABS_BUILD_DIR/bin:\$PATH"
EOF
  fi

  echo "Added build directory to PATH ($BUILD_DIR)"
fi
