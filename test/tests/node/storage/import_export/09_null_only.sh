#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

tenzir --neo --nova --bare-mode --console-verbosity=error \
  --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" '
  from {x: null}
  @name = "test.nullonly"
  import
'
