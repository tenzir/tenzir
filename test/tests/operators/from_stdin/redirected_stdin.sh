#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

tenzir --nova --bare-mode --console-verbosity=warning \
  'from_stdin { read_lines }' <"$TENZIR_INPUT"
tenzir --nova --bare-mode --console-verbosity=warning \
  'from_stdin { read_lines }' </dev/null
