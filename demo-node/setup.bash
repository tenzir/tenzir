#!/usr/bin/env bash

set -euo pipefail

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

echo "Spawnng M57 Suricata pipeline"
m57_suricata_definition='load https https://storage.googleapis.com/tenzir-datasets/M57/suricata.json.zst\n| decompress zstd\n| read suricata\n| where #schema != \"suricata.stats\"\n| import'
m57_suricata_id=$(tenzir -q "api /pipeline/create '{\"definition\": \"${m57_suricata_definition}\", \"name\": \"M57 Suricata\", \"autostart\": {\"created\": true}}'" | jq -re ".id")
echo "Setting labels for M57 Suricata pipeline"
m57_suricata_labels='[{"text": "suricata", "color": "#0086E4"}, {"text": "import", "color": "#2F00CC"}]'
tenzir -q "api /pipeline/update '{\"id\": \"${m57_suricata_id}\", \"labels\": ${m57_suricata_labels}}'" | jq -er '"id = \(.pipeline.id)"'

echo "Spawnng M57 Zeek pipeline"
m57_zeek_definition='load https https://storage.googleapis.com/tenzir-datasets/M57/zeek-all.log.zst\n| decompress zstd\n| read zeek-tsv\n| import'
m57_zeek_id=$(tenzir -q "api /pipeline/create '{\"definition\": \"${m57_zeek_definition}\", \"name\": \"M57 Zeek\", \"autostart\": {\"created\": true}}'" | jq -re ".id")
echo "Setting labels for M57 Zeek pipeline"
m57_zeek_labels='[{"text": "zeek", "color": "#E89400"}, {"text": "import", "color": "#2F00CC"}]'
tenzir -q "api /pipeline/update '{\"id\": \"${m57_zeek_id}\", \"labels\": ${m57_zeek_labels}}'" | jq -er '"id = \(.pipeline.id)"'

done="false"
while [[ "${done}" != "true" ]] ; do
  echo "Waiting for demo data import to complete..."
  sleep 5
  done=$(tenzir -q "api /pipeline/list" | jq ".pipelines | all(.state != \"running\")")
done

echo "Rebulding demo data import"
tenzir-ctl rebuild --undersized --all --parallel=4

kill "$NODE_PID"
wait "$NODE_PID"
