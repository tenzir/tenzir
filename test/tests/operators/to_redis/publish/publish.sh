#!/bin/sh
# runner: shell
set -eu

# TENZIR_BINARY holds a command line, such as `tenzir run`.
tenzir() {
  # shellcheck disable=SC2086
  $TENZIR_BINARY "$@"
}

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

# Waits until `cli <args>` prints the expected output.
wait_for() {
  expected=$1
  shift
  attempts=0
  until [ "$(cli "$@")" = "$expected" ]; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 300 ]; then
      echo "timed out waiting for \`$*\` to print \`$expected\`" >&2
      exit 1
    fi
    sleep 0.1
  done
}

out=$(mktemp)
trap 'rm -f "$out"' EXIT
tenzir --bare-mode --console-verbosity=warning \
  'from_redis "events", command="subscribe", url=env("REDIS_URL"), count=2
   this = string(message).parse_json()' >"$out" &
pid=$!
wait_for "$(printf 'events\n1')" pubsub numsub events
tenzir --bare-mode --console-verbosity=warning \
  'from {severity: "high"}, {severity: "low"}
   to_redis "events", command="publish", url=env("REDIS_URL")'
wait "$pid"
cat "$out"
