#!/bin/bash
set -e

# Simple status check
docker-compose run vast status

# Ingest Test Data
docker-compose run --rm --no-TTY vast import suricata < vast/integration/data/suricata/eve.json

# Query Ingested Test Data
result=$(docker-compose run --rm --no-TTY vast export json '147.32.84.165' | jq -s '. | length')

[ $result -eq 6 ]