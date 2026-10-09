#!/bin/sh
# runner: shell
# fixtures: [platform_telemetry]
# timeout: 120
set -eu

dir="$PLATFORM_TELEMETRY_DIR"

run() {
  id="$1"
  shift
  "$TENZIR_BINARY" --bare-mode --nova=true \
    --platform "$PLATFORM_TELEMETRY_URL" --deployment-id test-deployment \
    --key-file "$PLATFORM_TELEMETRY_KEY_FILE" --pipeline-id "$id" --run 7 "$@"
}

await_running() {
  i=0
  while [ ! -e "$dir/running-$1" ]; do
    i=$((i + 1))
    if [ "$i" -gt 100 ]; then
      echo "$1 did not report running"
      exit 1
    fi
    sleep 0.1
  done
}

printf '{}\n{}\n' | run short 'from_stdin { read_json } | discard' 2>/dev/null
if run compile-failure unknown_telemetry_operator 2>/dev/null; then
  echo "compile failure succeeded"
fi
run warnings \
  'from {x: 1}, {x: 2} | assert false, message="repeated" | discard' 2>/dev/null
if run runtime-failure "from_http \"$PLATFORM_TELEMETRY_URL/failed.json\", \
    max_retry_count=0 { read_json } | discard" 2>/dev/null; then
  echo "runtime failure succeeded"
fi

run stopped 'every 1h { from {x: 1} } | discard' 2>/dev/null &
pid=$!
await_running stopped
kill -TERM "$pid"
wait "$pid"

run aborted 'from {ts: 2020-01-01T00:00:00}
  | delay ts, start=2019-01-01T00:00:00
  | discard' 2>"$dir/aborted.err" &
pid=$!
await_running aborted
kill -TERM "$pid"
if wait "$pid"; then
  echo "aborted pipeline succeeded"
fi
grep -q "pipeline was aborted before completion" "$dir/aborted.err"
