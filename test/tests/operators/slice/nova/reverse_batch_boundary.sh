#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

seq 0 8192 |
  awk '{printf "{\"x\":%s}\n", $1}' |
  tenzir --nova --bare-mode --console-verbosity=warning \
    'load_stdin | read_json | slice stride=-1 | slice begin=8192'
