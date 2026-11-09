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

err=$(mktemp)
trap 'rm -f "$err"' EXIT
# Start the binary directly, so that the signal below reaches it.
# shellcheck disable=SC2086
$TENZIR_BINARY --bare-mode --console-verbosity=warning \
  'from_redis "queue", url=env("REDIS_URL")
   discard' 2>"$err" &
pid=$!
until cli client list | grep -q "cmd=brpop"; do
  sleep 0.1
done
# Stop the pipeline while it blocks in a pop, which takes at most a second.
kill -TERM "$pid"
wait "$pid"
# Nothing pops the item after the stop.
cli lpush queue later >/dev/null
sleep 1.5
cli lrange queue 0 -1
