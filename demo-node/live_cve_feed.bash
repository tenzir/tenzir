#!/usr/bin/env bash

set -euo pipefail

OUTPUT=$(curl --retry 100 -s "https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=1")
LAST=$(($(echo "$OUTPUT" | jq '.totalResults') - 1000))
while true
do
  OUTPUT=$(curl --retry 100 -s "https://services.nvd.nist.gov/rest/json/cves/2.0?startIndex=${LAST}")
  echo "$OUTPUT" | jq -c '.vulnerabilities[].cve'
  LAST=$(($LAST + $(echo "$OUTPUT" | jq -c '.vulnerabilities | length')))
  sleep 10
done
