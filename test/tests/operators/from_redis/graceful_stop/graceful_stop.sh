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
# Start the binary directly, so that the signals below reach it.
# shellcheck disable=SC2086
$TENZIR_BINARY --bare-mode --console-verbosity=warning \
  'from_redis "queue", url=env("REDIS_URL")
   select item=string(message)' >"$out" 2>/dev/null &
pid=$!
until cli client list | grep -q "cmd=brpop"; do
  sleep 0.1
done
# Hold back writes, including the next blocking pop of `from_redis`, which
# Redis then serves right after the pause with the item that we push now.
cli client pause 3000 write >/dev/null
sleep 1.5
cli lpush queue in-flight second third >/dev/null &
sleep 0.3
# A graceful stop lets the pop in flight finish and emits its item.
kill -TERM "$pid"
wait "$pid"
cat "$out"
# The stop also keeps the operator from taking the other items with the pop.
cli lrange queue 0 -1
