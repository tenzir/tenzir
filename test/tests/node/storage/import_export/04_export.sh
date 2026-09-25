#!/bin/sh
# runner: shell
set -eu

"$TENZIR_BINARY" --neo --nova --bare-mode --console-verbosity=error \
  --endpoint="$TENZIR_NODE_CLIENT_ENDPOINT" '
  export
  where @name == "test.events"
  sort id
  write_json
'
