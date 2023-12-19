#!/usr/bin/env bash

set -euo pipefail

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node -vv --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" TENZIR_ENDPOINT

# Empirically, we sometimes fail to connect to a node if we attempt that right
# after starting it up. So we simply wait a bit with that.
IFS=':' read -r -a hostname_and_port <<< "${TENZIR_ENDPOINT}"
while ! lsof -i ":${hostname_and_port[1]}"; do
  echo "Waiting for node to be reachable..."
  sleep 1
done

echo "Spawning M57 Suricata pipeline"
m57_suricata_definition='from https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst read suricata --no-infer\n| where #schema != \"suricata.stats\"\n| import'
m57_suricata_id=$(tenzir -q "api /pipeline/create '{\"definition\": \"${m57_suricata_definition}\", \"name\": \"M57 Suricata\", \"autostart\": {\"created\": true}}'" | jq -re ".id")
echo "Setting labels for M57 Suricata pipeline"
m57_suricata_labels='[{"text": "suricata", "color": "#0086e5"}, {"text": "import", "color": "#2f00cc"}]'
tenzir -q "api /pipeline/update '{\"id\": \"${m57_suricata_id}\", \"labels\": ${m57_suricata_labels}}'" | jq -er '"id = \(.pipeline.id)"'

done="false"
while [[ "${done}" != "true" ]] ; do
  echo "Waiting for M57 Suricata demo data import to complete..."
  sleep 5
  done=$(tenzir -q "api /pipeline/list" | jq ".pipelines | all(.state != \"running\")")
done

echo "Spawning M57 Zeek pipeline"
m57_zeek_definition='from https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst read zeek-tsv\n| import'
m57_zeek_id=$(tenzir -q "api /pipeline/create '{\"definition\": \"${m57_zeek_definition}\", \"name\": \"M57 Zeek\", \"autostart\": {\"created\": true}}'" | jq -re ".id")
echo "Setting labels for M57 Zeek pipeline"
m57_zeek_labels='[{"text": "zeek", "color": "#e89400"}, {"text": "import", "color": "#2f00cc"}]'
tenzir -q "api /pipeline/update '{\"id\": \"${m57_zeek_id}\", \"labels\": ${m57_zeek_labels}}'" | jq -er '"id = \(.pipeline.id)"'

done="false"
while [[ "${done}" != "true" ]] ; do
  echo "Waiting for M57 Zeek demo data import to complete..."
  sleep 5
  done=$(tenzir -q "api /pipeline/list" | jq ".pipelines | all(.state != \"running\")")
done

echo "Rebuilding demo data"
tenzir-ctl rebuild --undersized --all --parallel=4

kill "$NODE_PID"
wait "$NODE_PID"
