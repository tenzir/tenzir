#!/bin/sh
# runner: shell
# timeout: 180
set -eu

scratch=$(mktemp -d)
node_pid=
cleanup() {
  if [ -n "$node_pid" ]; then
    kill -TERM "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
  fi
  rm -r "$scratch"
}
trap cleanup EXIT
mkdir -p "$scratch/state" "$scratch/cache"

start_node() {
  : >"$scratch/endpoint"
  "$TENZIR_NODE_BINARY" --bare-mode --console-verbosity=error \
    --state-directory="$scratch/state" --cache-directory="$scratch/cache" \
    --endpoint=localhost:0 --print-endpoint --no-autostart \
    >"$scratch/endpoint" 2>"$scratch/node.log" &
  node_pid=$!
  attempts=0
  while [ ! -s "$scratch/endpoint" ] && [ "$attempts" -lt 300 ]; do
    sleep 0.1
    attempts=$((attempts + 1))
  done
  endpoint=$(head -n 1 "$scratch/endpoint")
  if [ -z "$endpoint" ]; then
    echo "node did not publish an endpoint" >&2
    exit 1
  fi
}

stop_node() {
  kill -TERM "$node_pid"
  wait "$node_pid" || true
  node_pid=
}

start_node
"$TENZIR_BINARY" --neo --nova --bare-mode --console-verbosity=error \
  --endpoint="$endpoint" '
  from {id: 1, x: null}, {id: 2, x: "a"}, {id: 3, x: 2}
  @name = "restart.events"
  import
'
stop_node
rm -f "$scratch/state/pid.lock"
start_node
"$TENZIR_BINARY" --neo --nova --bare-mode --console-verbosity=error \
  --endpoint="$endpoint" '
  export
  where @name == "restart.events"
  sort id
  write_ndjson
'
