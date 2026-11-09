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

log=$(mktemp)
trap 'rm -f "$log"' EXIT
# Record every command that the server runs.
# shellcheck disable=SC2086
$REDIS_CLI monitor >"$log" &
monitor=$!
until grep -q OK "$log"; do
  sleep 0.1
done
tenzir --bare-mode --console-verbosity=warning \
  'from {job: "a"}, {job: "b"}
   to_redis "jobs", command="rpush", message=job, ttl=1h, url=env("REDIS_URL")'
kill "$monitor"
wait "$monitor" 2>/dev/null || true
# The write and its expiry run together in one script, so that no
# interruption can separate them. A client sends no separate PEXPIRE.
echo "separate expiries: $(grep -c '[0-9]\] "PEXPIRE"' "$log" || true)"
echo "scripted writes: $(grep -c '[0-9]\] "EVAL"' "$log" || true)"
cli lrange jobs 0 -1
[ "$(cli ttl jobs)" -gt 0 ] && echo "has ttl"
