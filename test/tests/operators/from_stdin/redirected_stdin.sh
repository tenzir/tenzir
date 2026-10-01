#!/bin/sh
# runner: shell
set -eu

"$TENZIR_BINARY" --nova --bare-mode --console-verbosity=warning \
  'from_stdin { read_lines }' <"$TENZIR_INPUT"
"$TENZIR_BINARY" --nova --bare-mode --console-verbosity=warning \
  'from_stdin { read_lines }' </dev/null
