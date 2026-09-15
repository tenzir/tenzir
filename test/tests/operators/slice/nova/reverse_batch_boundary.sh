#!/bin/sh
# runner: shell
set -eu

seq 0 8192 |
  awk '{printf "{\"x\":%s}\n", $1}' |
  "$TENZIR_BINARY" --nova --bare-mode --console-verbosity=warning \
    'load_stdin | read_json | slice stride=-1 | slice begin=8192'
