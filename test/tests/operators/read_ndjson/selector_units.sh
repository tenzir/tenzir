#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

# Both schemas define `ts` as time, in different units.
printf '{"kind":"a","ts":1}\n{"kind":"b","ts":1000}\n' |
  tenzir --nova --bare-mode --console-verbosity=warning \
    --schema-dirs="$(dirname "$0")/schemas" \
    'load_stdin | read_ndjson selector="kind:unit" | select schema=@name, ts'
