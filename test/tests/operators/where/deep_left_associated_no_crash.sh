#!/bin/sh
# runner: shell
set -eu

dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
stdout_file=$(mktemp)
stderr_file=$(mktemp)
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

exit_code=0
$TENZIR_BINARY --bare-mode --console-verbosity=warning \
  -f "$dir/deep_left_associated_no_crash.tql.in" >"$stdout_file" \
  2>"$stderr_file" || exit_code=$?
if [ "$exit_code" -ge 128 ]; then
  echo "process killed by signal $((exit_code - 128))"
  exit 1
fi
test "$exit_code" -eq 0
test ! -s "$stdout_file"
test ! -s "$stderr_file"
