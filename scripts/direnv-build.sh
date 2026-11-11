# shellcheck shell=bash

# Prepend the selected engine build's bin/ to PATH.
if tenzir_build_dir=$(./build.sh --print-build-dir 2>/dev/null); then
  PATH_add "$tenzir_build_dir/bin"
fi
unset tenzir_build_dir
