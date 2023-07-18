#!/usr/bin/env bash

export TENZIR_AUTOMATIC_REBUILD="${TENZIR_AUTOMATIC_REBUILD:-0}"
export TENZIR_ALLOW_UNSAFE_PIPELINES=true

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

tenzir 'read suricata | where #schema != "suricata.stats" | import' < Suricata/eve.json
cat Zeek/*.log | tenzir 'read zeek-tsv | import'
rm -rf Suricata
rm -rf Zeek

tenzir-ctl flush
tenzir-ctl rebuild --undersized
kill "$NODE_PID"
wait "$NODE_PID"
