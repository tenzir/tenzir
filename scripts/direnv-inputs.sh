# shellcheck shell=bash

# Watches the inputs of the engine's development shell. Every `.envrc` that
# loads it sources this, so direnv reloads and TENZIR_DIRENV_CUSTOM sees the
# complete list. `source_env` runs this from `scripts/`.
cd .. || return
watch_file shell.nix flake.lock
watch_file nix nix/overlay.nix nix/dependencies.nix nix/test-dependencies.nix \
  nix/python-dependencies.nix nix/overrides/* nix/caf/
watch_file python plugins/iceberg/aux/iceberg-cpp.patch
