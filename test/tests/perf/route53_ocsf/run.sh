#!/usr/bin/env bash

set -euo pipefail

# Reproducible Route53 OCSF benchmark using the canonical perf fixture setup.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../../.." && pwd)

TENZIR_BIN=${TENZIR_BIN:-tenzir}
COUNT=${COUNT:-1M}
PARTITIONS=${PARTITIONS:-4}
MESSAGES=${MESSAGES:-1000000}
COMPRESSION=${COMPRESSION:-none}
PAYLOAD_FILE=${PAYLOAD_FILE:-$SCRIPT_DIR/route53_sample.ndjson}
TOPIC=${TOPIC:-route53_ocsf_perf_1m}
GROUP_ID=${GROUP_ID:-tenzir-perf-route53-ocsf-$(date +%s)}

fixture_log=$(mktemp)
pipeline_file=$(mktemp)
fixture_pid=""
KAFKA_BOOTSTRAP_SERVERS=""
KAFKA_TOPIC=""

cleanup() {
  if [[ -n "$fixture_pid" ]] && kill -0 "$fixture_pid" 2>/dev/null; then
    kill -INT "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" || true
  fi
  rm -f "$fixture_log" "$pipeline_file"
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

fixture_options="kafka: {topic: $TOPIC, partitions: $PARTITIONS, messages: $MESSAGES, compression: \"$COMPRESSION\"}"
if [[ -n "$PAYLOAD_FILE" ]]; then
  fixture_options="kafka: {topic: $TOPIC, partitions: $PARTITIONS, messages: $MESSAGES, compression: \"$COMPRESSION\", payload_file: \"$PAYLOAD_FILE\"}"
fi

uvx tenzir-test --root test \
  --fixture "$fixture_options" \
  --debug >"$fixture_log" 2>&1 &
fixture_pid=$!

# Wait until fixture prints environment variables.
ready=0
for _ in {1..240}; do
  if grep -q '^KAFKA_BOOTSTRAP_SERVERS=' "$fixture_log" &&
    grep -q '^KAFKA_TOPIC=' "$fixture_log"; then
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

# Build the pipeline from the TQL template, substituting env vars.
export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC

echo "fixture: bootstrap=$KAFKA_BOOTSTRAP_SERVERS topic=$KAFKA_TOPIC partitions=$PARTITIONS messages=$MESSAGES compression=$COMPRESSION payload_file=${PAYLOAD_FILE:-none}"
echo "bench: count=$COUNT group_id=$GROUP_ID"

/usr/bin/time -p "$TENZIR_BIN" --neo -f "$SCRIPT_DIR/route53_ocsf.tql"
