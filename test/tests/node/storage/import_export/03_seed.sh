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
  from {id: 1, x: null, y: 1},
       {id: 2, x: "a", y: null},
       {id: 3, x: 2, y: 3},
       {id: 4, x: null, y: null},
       {}
  @name = "test.events"
  import
'
