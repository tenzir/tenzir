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

# Read data from eth0. Currently disable because that interface sees very little
# traffic, giving the impression that the pipeline is defunct.
# nic_pipe="from nic eth0 | decapsulate data | import"
# curl -X POST \
#   -H "Content-Type: application/json" \https://services.nvd.nist.gov/rest/json/cves/2.0
#   -d "{\"name\": \"Example eth0 Import\", \"definition\": \"${nic_pipe}\", \"start_when_created\": false}" \
#   http://127.0.0.1:5160/api/v0/pipeline/create

# Continuously import system load data from `vmstat -a -n 1`.
stat_pipe="shell /demo-node/csvstat.sh | read csv | replace #schema=\"vmstat.all\" | unflatten | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"System Load\", \"definition\": \"${stat_pipe}\", \"start_when_created\": true}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

# Ingest CVEs from https://services.nvd.nist.gov/rest/json/cves/2.0.
cve_pipe="shell /demo-node/live_cve_feed.bash | read json --ndjson | replace #schema=\"nvd.cve\" | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"Live CVE Notifications from the NIST API\", \"definition\": \"${cve_pipe}\", \"start_when_created\": true}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

# The shell operator decompresses the data and writes it to `read suricata` on
# the fly.
# !! Not started because the data has already been imported while building the image.
suricata_pipe="shell 'bash -c \\\"curl -s -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.zst | tar -x --zstd --to-stdout\\\"' | read suricata | where #schema != \\\"suricata.stats\\\" | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"M57 Suricata Import\", \"definition\": \"${suricata_pipe}\", \"start_when_created\": false}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

# The shell operator "streams" the data into a `Zeek/` directory on disk, calls
# `sync` to flush the OS write buffers, and finally sends the concatenation of
# the logs into `read zeek-tsv`.
# !! Not started because the data has already been imported while building the image.
zeek_pipe="shell 'bash -c \\\"curl -s -L https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.zst | tar -x --zstd; sync; cat Zeek/*.log\\\"' | read zeek-tsv | import"
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"M57 Zeek Import\", \"definition\": \"${zeek_pipe}\", \"start_when_created\": false}" \
  http://127.0.0.1:5160/api/v0/pipeline/create

wait "$NODE_PID"
