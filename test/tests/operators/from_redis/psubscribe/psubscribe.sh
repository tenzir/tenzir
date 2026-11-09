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
  'from_redis "alerts.*", command="psubscribe", url=env("REDIS_URL"), count=2,
     metadata_field=redis
   select message=string(message), channel=redis.channel,
     pattern=redis.pattern' >"$out" &
pid=$!
wait_for 1 pubsub numpat
cli publish alerts.high first >/dev/null
cli publish other ignored >/dev/null
cli publish alerts.low second >/dev/null
wait "$pid"
cat "$out"
