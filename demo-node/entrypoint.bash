#!/usr/bin/env bash

export TENZIR_AUTOMATIC_REBUILD="${TENZIR_AUTOMATIC_REBUILD:-0}"

coproc NODE { exec tenzir-node --print-endpoint --commands="web server --mode=dev"; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

# Reset the verbosity so the logs don't get spammed by dozens
# of Tenzir processes.
export TENZIR_CONSOLE_VERBOSITY="${TENZIR_CLIENT_CONSOLE_VERBOSITY:-"info"}"

suricata_pipe="shell \'bash -c \"curl -s -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.zst | tar -x --zstd --to-stdout\"\' | parse suricata | import"
zeek_pipe="shell \'bash -c \"curl -s -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.zst | tar -x --zstd; cat Zeek/*.log\"\' | parse zeek | import"

curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"definition\": \"${suricata_pipe}\", \"start_when_created\": true}" \
  http://localhost:5160/api/v0/pipeline/create

curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"definition\": \"${zeek_pipe}\", \"start_when_created\": true}" \
  http://localhost:5160/api/v0/pipeline/create

wait "$NODE_PID"
