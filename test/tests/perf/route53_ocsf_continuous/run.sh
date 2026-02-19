#!/usr/bin/env bash

set -euo pipefail

# Continuous Route53 OCSF benchmark:
# - Starts the kafka fixture
# - Runs the kafka-produce.py producer in the background
# - Runs the tenzir pipeline in the background
# - Periodically prints kafka consumer group lag statistics

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../../.." && pwd)

TENZIR_BIN=${TENZIR_BIN:-tenzir}
PRODUCER_SCRIPT=${PRODUCER_SCRIPT:-~/tenzir/atlassian-benchmark/kafka-produce.py}
PRODUCER_FILE=${PRODUCER_FILE:-$SCRIPT_DIR/route53_sample.ndjson}
PARTITIONS=${PARTITIONS:-4}
COMPRESSION=${COMPRESSION:-none}
TOPIC=${TOPIC:-route53_ocsf_continuous}
OUTPUT_TOPIC=${OUTPUT_TOPIC:-route53-ocsf-output}
STATS_INTERVAL=${STATS_INTERVAL:-10}

fixture_log=$(mktemp)
fixture_pid=""
producer_pid=""
tenzir_pid=""
KAFKA_BOOTSTRAP_SERVERS=""
KAFKA_TOPIC=""
KAFKA_CONTAINER_ID=""
KAFKA_CONTAINER_RUNTIME=""

cleanup() {
  echo ""
  echo "=== shutting down ==="
  for pid_var in tenzir_pid producer_pid fixture_pid; do
    pid=${!pid_var}
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -f "$fixture_log"
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

# --- Start kafka fixture (no pre-seeding, producer handles that) ---
fixture_options="kafka: {topic: $TOPIC, partitions: $PARTITIONS, messages: 0, compression: \"$COMPRESSION\"}"

uvx tenzir-test --root test \
  --fixture "$fixture_options" \
  --debug >"$fixture_log" 2>&1 &
fixture_pid=$!

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
KAFKA_CONTAINER_ID=$(grep '^KAFKA_CONTAINER_ID=' "$fixture_log" | tail -n1 | cut -d= -f2-)
KAFKA_CONTAINER_RUNTIME=$(grep '^KAFKA_CONTAINER_RUNTIME=' "$fixture_log" | tail -n1 | cut -d= -f2-)

export KAFKA_BOOTSTRAP_SERVERS KAFKA_TOPIC

echo "=== kafka fixture ready ==="
echo "  bootstrap: $KAFKA_BOOTSTRAP_SERVERS"
echo "  topic:     $KAFKA_TOPIC"
echo "  container: ${KAFKA_CONTAINER_ID:0:12}"
echo ""

# --- Start the kafka producer in the background ---
echo "=== starting kafka producer ==="
uv run "$PRODUCER_SCRIPT" "$KAFKA_TOPIC" \
  --bootstrap-servers "$KAFKA_BOOTSTRAP_SERVERS" \
  --file "$PRODUCER_FILE" \
  --partitions "$PARTITIONS" \
  --compression "$COMPRESSION" \
  --batch-delay 0 &
producer_pid=$!
echo "  producer pid: $producer_pid"
echo ""

# Give the producer a head start to buffer some messages.
sleep 3

# --- Start tenzir pipeline in the background ---
echo "=== starting tenzir pipeline ==="
"$TENZIR_BIN" --neo -f "$SCRIPT_DIR/route53_ocsf_continuous.tql" &
tenzir_pid=$!
echo "  tenzir pid: $tenzir_pid"
echo ""

# --- Helper: get total bytes for a topic from kafka log dirs ---
get_topic_bytes() {
  local topic=$1
  "$KAFKA_CONTAINER_RUNTIME" exec "$KAFKA_CONTAINER_ID" \
    /opt/kafka/bin/kafka-log-dirs.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic-list "$topic" 2>/dev/null |
    grep -oP '"size":\K[0-9]+' |
    awk '{s+=$1} END {print s+0}'
}

# --- Helper: get total offset (messages) across all partitions ---
get_topic_offsets() {
  local topic=$1
  "$KAFKA_CONTAINER_RUNTIME" exec "$KAFKA_CONTAINER_ID" \
    /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic "$topic" 2>/dev/null |
    awk -F: '{s+=$3} END {print s+0}'
}

format_bytes() {
  local bytes=$1
  if ((bytes >= 1073741824)); then
    awk "BEGIN {printf \"%.2f GB\", $bytes/1073741824}"
  elif ((bytes >= 1048576)); then
    awk "BEGIN {printf \"%.2f MB\", $bytes/1048576}"
  elif ((bytes >= 1024)); then
    awk "BEGIN {printf \"%.2f KB\", $bytes/1024}"
  else
    echo "${bytes} B"
  fi
}

# --- Periodically print kafka stats ---
echo "=== stats (every ${STATS_INTERVAL}s) ==="
prev_in_bytes=0
prev_out_bytes=0
prev_in_msgs=0
prev_out_msgs=0
prev_time=$(date +%s)

while true; do
  sleep "$STATS_INTERVAL"

  # Check that child processes are still alive.
  if ! kill -0 "$tenzir_pid" 2>/dev/null; then
    echo "tenzir pipeline exited"
    break
  fi
  if ! kill -0 "$producer_pid" 2>/dev/null; then
    echo "producer exited"
    break
  fi

  now=$(date +%s)
  elapsed=$((now - prev_time))

  in_bytes=$(get_topic_bytes "$KAFKA_TOPIC")
  out_bytes=$(get_topic_bytes "$OUTPUT_TOPIC")
  in_msgs=$(get_topic_offsets "$KAFKA_TOPIC")
  out_msgs=$(get_topic_offsets "$OUTPUT_TOPIC")

  delta_in_bytes=$((in_bytes - prev_in_bytes))
  delta_out_bytes=$((out_bytes - prev_out_bytes))
  delta_in_msgs=$((in_msgs - prev_in_msgs))
  delta_out_msgs=$((out_msgs - prev_out_msgs))

  if ((elapsed > 0)); then
    in_bytes_per_s=$((delta_in_bytes / elapsed))
    out_bytes_per_s=$((delta_out_bytes / elapsed))
    in_msgs_per_s=$((delta_in_msgs / elapsed))
    out_msgs_per_s=$((delta_out_msgs / elapsed))
  else
    in_bytes_per_s=0
    out_bytes_per_s=0
    in_msgs_per_s=0
    out_msgs_per_s=0
  fi

  echo "--- $(date +%H:%M:%S) ---"
  printf "  IN  %-14s  total: %-14s  msgs: %s (%s/s)\n" \
    "$(format_bytes $in_bytes_per_s)/s" \
    "$(format_bytes $in_bytes)" \
    "$in_msgs" "$in_msgs_per_s"
  printf "  OUT %-14s  total: %-14s  msgs: %s (%s/s)\n" \
    "$(format_bytes $out_bytes_per_s)/s" \
    "$(format_bytes $out_bytes)" \
    "$out_msgs" "$out_msgs_per_s"
  printf "  LAG %s msgs\n" "$((in_msgs - out_msgs))"

  prev_in_bytes=$in_bytes
  prev_out_bytes=$out_bytes
  prev_in_msgs=$in_msgs
  prev_out_msgs=$out_msgs
  prev_time=$now

  echo ""
done
