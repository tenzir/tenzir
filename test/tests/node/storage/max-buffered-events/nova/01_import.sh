#!/bin/sh
# runner: shell
set -eu

"$TENZIR_BINARY" --neo --nova --bare-mode --console-verbosity=error \
  --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" '
  from {x: 1}
  repeat 10
  @name = "buffered.nova"
  import
'
