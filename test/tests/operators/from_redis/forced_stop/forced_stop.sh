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

# Start the binary directly, so that the signals below reach it.
# shellcheck disable=SC2086
$TENZIR_BINARY --bare-mode --console-verbosity=error \
  'from_redis "queue", url=env("REDIS_URL")
   discard' 2>/dev/null &
pid=$!
until cli client list | grep -q "cmd=brpop"; do
  sleep 0.1
done
# Stall the next blocking pop of `from_redis` on the server.
cli client pause 10000 write >/dev/null
sleep 1.5
kill -TERM "$pid"
sleep 0.3
# A second signal cancels the pipeline, which must not wait for the stalled
# pop.
kill -TERM "$pid"
attempts=0
while kill -0 "$pid" 2>/dev/null; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 30 ]; then
    echo "still running 3 seconds after the second signal"
    kill -KILL "$pid"
    break
  fi
  sleep 0.1
done
wait "$pid" || true
cli client unpause >/dev/null
# The abandoned pop never ran, so the item stays in the list.
cli lpush queue later >/dev/null
sleep 1.5
cli lrange queue 0 -1
