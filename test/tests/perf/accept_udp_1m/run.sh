#!/usr/bin/env bash

set -euo pipefail

# Reproducible accept_udp benchmark using the canonical UDP test fixture.
# Set GENERATOR=udpgen and UDPGEN_BIN=/path/to/udpgen for a native sender.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../../.." && pwd)

TENZIR_BIN=${TENZIR_BIN:-tenzir}
COUNT=${COUNT:-1000000}
PAYLOAD=${PAYLOAD:-route53-benchmark-payload}
PAYLOAD_HEX=${PAYLOAD_HEX:-}
INTERVAL=${INTERVAL:-0.0}
INITIAL_DELAY=${INITIAL_DELAY:-1.0}
BINARY=${BINARY:-false}
SINK=${SINK:-discard}
GENERATOR=${GENERATOR:-fixture}
UDPGEN_BIN=${UDPGEN_BIN:-udpgen}
UDPGEN_RATE=${UDPGEN_RATE:-0}
UDPGEN_THREADS=${UDPGEN_THREADS:-1}
UDPGEN_PACKETS_PER_SEND=${UDPGEN_PACKETS_PER_SEND:-100}
UDPGEN_COUNT=${UDPGEN_COUNT:-$COUNT}

fixture_log=$(mktemp)
pipeline_file=$(mktemp)
fixture_pid=""
generator_pid=""
UDP_ENDPOINT=""

cleanup() {
  if [[ -n $fixture_pid ]] && kill -0 "$fixture_pid" 2>/dev/null; then
    kill -INT "$fixture_pid" 2>/dev/null || true
    wait "$fixture_pid" || true
  fi
  if [[ -n $generator_pid ]] && kill -0 "$generator_pid" 2>/dev/null; then
    kill -INT "$generator_pid" 2>/dev/null || true
    wait "$generator_pid" || true
  fi
  rm -f "$fixture_log" "$pipeline_file"
}
trap cleanup EXIT INT TERM

cd "$ROOT_DIR"

if [[ $GENERATOR == fixture ]]; then
  if [[ -n $PAYLOAD_HEX ]]; then
    fixture_options="udp: {mode: client, payload_hex: \"$PAYLOAD_HEX\", interval: $INTERVAL, initial_delay: $INITIAL_DELAY}"
  else
    fixture_options="udp: {mode: client, payload: \"$PAYLOAD\", interval: $INTERVAL, initial_delay: $INITIAL_DELAY}"
  fi

  uvx tenzir-test --root test \
    --fixture "$fixture_options" \
    --debug >"$fixture_log" 2>&1 &
  fixture_pid=$!

  # Wait until the fixture prints environment variables.
  ready=0
  for _ in {1..240}; do
    if grep -q '^UDP_ENDPOINT=' "$fixture_log"; then
      ready=1
      break
    fi
    if ! kill -0 "$fixture_pid" 2>/dev/null; then
      break
    fi
    sleep 0.5
  done

  if [[ $ready -ne 1 ]]; then
    echo "error: udp fixture did not become ready" >&2
    sed -n '1,200p' "$fixture_log" >&2
    exit 1
  fi

  UDP_ENDPOINT=$(grep '^UDP_ENDPOINT=' "$fixture_log" | tail -n1 | cut -d= -f2-)
elif [[ $GENERATOR == udpgen ]]; then
  if ! command -v "$UDPGEN_BIN" >/dev/null 2>&1; then
    echo "error: udpgen binary not found: $UDPGEN_BIN" >&2
    exit 1
  fi
  port=$(
    python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
  )
  UDP_ENDPOINT="127.0.0.1:$port"
else
  echo "error: GENERATOR must be one of: fixture, udpgen" >&2
  exit 1
fi

cat >"$pipeline_file" <<PIPELINE
accept_udp "$UDP_ENDPOINT", binary=$BINARY
head $COUNT
$SINK
PIPELINE

echo "generator: $GENERATOR endpoint=$UDP_ENDPOINT binary=$BINARY initial_delay=$INITIAL_DELAY"
if [[ $GENERATOR == fixture ]]; then
  echo "fixture: payload=${PAYLOAD_HEX:-$PAYLOAD} interval=$INTERVAL"
else
  echo "udpgen: bin=$UDPGEN_BIN rate=$UDPGEN_RATE threads=$UDPGEN_THREADS packets_per_send=$UDPGEN_PACKETS_PER_SEND count=$UDPGEN_COUNT"
  (
    sleep "$INITIAL_DELAY"
    "$UDPGEN_BIN" -i -h "${UDP_ENDPOINT%:*}" -p "${UDP_ENDPOINT##*:}" \
      -r "$UDPGEN_RATE" -s "$UDPGEN_COUNT" -t "$UDPGEN_THREADS" \
      -z "$UDPGEN_PACKETS_PER_SEND"
  ) &
  generator_pid=$!
fi
echo "bench: count=$COUNT sink=$SINK"

/usr/bin/time -p "$TENZIR_BIN" --neo -f "$pipeline_file"
