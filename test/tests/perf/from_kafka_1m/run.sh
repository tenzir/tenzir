#!/usr/bin/env bash

set -euo pipefail

# Reproducible from_kafka 1M benchmark using the canonical perf fixture setup.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../../.." && pwd)

TENZIR_BIN=${TENZIR_BIN:-tenzir}
COUNT=${COUNT:-1M}
BATCH_SIZE=${BATCH_SIZE:-50k}
BATCH_TIMEOUT=${BATCH_TIMEOUT:-1s}
FETCH_WAIT_TIMEOUT=${FETCH_WAIT_TIMEOUT:-16ms}
SINK=${SINK:-discard}
PARTITIONS=${PARTITIONS:-1}
SEED_MESSAGES=${SEED_MESSAGES:-1000000}
TOPIC=${TOPIC:-from_kafka_perf_1m}
GROUP_ID=${GROUP_ID:-tenzir-perf-from-kafka-1m-$(date +%s)}

fixture_log=$(mktemp)
pipeline_file=$(mktemp)
fixture_pid=""

cleanup() {
  if [[ -n "$fixture_pid" ]] && kill -0 "$fixture_pid" 2>/dev/null; then
    kill -INT "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" || true
  fi
  rm -f "$fixture_log" "$pipeline_file"
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

uvx tenzir-test --root test \
  --fixture "kafka: {topic: $TOPIC, partitions: $PARTITIONS, seed_messages: $SEED_MESSAGES}" \
  --debug >"$fixture_log" 2>&1 &
fixture_pid=$!

# Wait until fixture prints environment variables.
ready=0
for _ in {1..240}; do
  if grep -q '^KAFKA_BOOTSTRAP_SERVERS=' "$fixture_log" \
    && grep -q '^KAFKA_TOPIC=' "$fixture_log"; then
    ready=1
    break
  fi
  if ! kill -0 "$fixture_pid" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

if [[ $ready -ne 1 ]]; then
  echo "error: kafka fixture did not become ready" >&2
  sed -n '1,200p' "$fixture_log" >&2
  exit 1
fi

KAFKA_BOOTSTRAP_SERVERS=$(grep '^KAFKA_BOOTSTRAP_SERVERS=' "$fixture_log" | tail -n1 | cut -d= -f2-)
KAFKA_TOPIC=$(grep '^KAFKA_TOPIC=' "$fixture_log" | tail -n1 | cut -d= -f2-)

cat >"$pipeline_file" <<PIPELINE
from_kafka "$KAFKA_TOPIC",
           count=$COUNT,
           offset="beginning",
           batch_size=$BATCH_SIZE,
           batch_timeout=$BATCH_TIMEOUT,
           fetch_wait_timeout=$FETCH_WAIT_TIMEOUT,
           options={
             "bootstrap.servers": "$KAFKA_BOOTSTRAP_SERVERS",
             "group.id": "$GROUP_ID"
           }
$SINK
PIPELINE

echo "fixture: bootstrap=$KAFKA_BOOTSTRAP_SERVERS topic=$KAFKA_TOPIC partitions=$PARTITIONS seed_messages=$SEED_MESSAGES"
echo "bench: count=$COUNT batch_size=$BATCH_SIZE batch_timeout=$BATCH_TIMEOUT fetch_wait_timeout=$FETCH_WAIT_TIMEOUT sink=$SINK group_id=$GROUP_ID"

TENZIR_KAFKA_FROM_PERF_STATS=1 /usr/bin/time -p "$TENZIR_BIN" --neo -f "$pipeline_file"
