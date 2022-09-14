#!/bin/bash
set -eux -o pipefail

pushd "$(git -C "$(dirname "$(readlink -f "${0}")")" rev-parse --show-toplevel)"

# Simple status check
docker compose run --rm vast status

# Ingest Test Data
docker compose run --rm --no-TTY vast import suricata < vast/integration/data/suricata/eve.json

# Query Ingested Test Data
docker compose run --rm --interactive=false vast export json '147.32.84.165' | jq -es '. | length == 6'

popd