#!/usr/bin/env bash

set -e

export TENZIR_AUTOMATIC_REBUILD="${TENZIR_AUTOMATIC_REBUILD:-0}"
export TENZIR_ALLOW_UNSAFE_PIPELINES=true

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

tenzir 'load https https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst | decompress zstd | read suricata | where #schema != "suricata.stats" | import'
tenzir 'load https https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst | decompress zstd | read zeek-tsv | import'

tenzir-ctl flush
tenzir-ctl rebuild --undersized
kill "$NODE_PID"
wait "$NODE_PID"
