#!/usr/bin/env bash

export TENZIR_AUTOMATIC_REBUILD="${TENZIR_AUTOMATIC_REBUILD:-0}"
export TENZIR_ALLOW_UNSAFE_PIPELINES=true

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node --print-endpoint --commands="web server --mode=dev"; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

# Reset the verbosity so the logs don't get spammed by dozens
# of Tenzir processes.
export TENZIR_CONSOLE_VERBOSITY="${TENZIR_CLIENT_CONSOLE_VERBOSITY:-"info"}"

# Wait until the HTTP endpoint is listening.
while ! timeout 100 bash -c "echo > /dev/tcp/127.0.0.1/5160"; do
  echo "Waiting for the HTTP service..."
  sleep 1
done

nic_pipe="from nic eth0 | drop data | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"Example eth0 Import\", \"definition\": \"${nic_pipe}\", \"start_when_created\": false}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

stat_pipe="shell /demo-node/csvstat.sh | read csv | unflatten | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"Example vmstat import\", \"definition\": \"${stat_pipe}\", \"start_when_created\": false}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

wait "$NODE_PID"
