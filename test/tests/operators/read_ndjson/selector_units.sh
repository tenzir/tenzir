#!/bin/sh
# runner: shell
set -eu

# Both schemas define `ts` as time, in different units.
printf '{"kind":"a","ts":1}\n{"kind":"b","ts":1000}\n' |
  "$TENZIR_BINARY" --nova --bare-mode --console-verbosity=warning \
    --schema-dirs="$(dirname "$0")/schemas" \
    'load_stdin | read_ndjson selector="kind:unit" | select schema=@name, ts'
