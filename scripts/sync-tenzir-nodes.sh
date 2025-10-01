#!/usr/bin/env bash

# A script to synchronize two Tenzir instances, possibly with an assoicated query
# for selectively synchronizing based on specific properties or types
#
# Usage examples:
#   sync-tenzir-nodes.sh "#schema == \"suricata.alert\""  10 :5158 :5160 tenzir
#   sync-tenzir-nodes.sh "" 10 :5158 :5160 tenzir
#
# Arguments
#   query: e.g. "#schema == \"suricata.alert\"" - mind the quotes; can be empty ""
#   seconds - sync interval in seconds - default 5
#   Tenzir endpoint of source - default :5158
#   Tenzir endpoint of sink   - default :5160
#   path to Tenzir binary     - default `tenzir` (must be in path)

TS_FILE="last_queried.ts"

Q=${1:-""}
S=${2:-5}
SOURCE_TENZIR_ENDPOINT=${3:-:5158}
DESTINATION_TENZIR_ENDPOINT=${4:-:5160}
TENZIR_BINARY="${5:-tenzir}"

>&2 echo "syncing $SOURCE_TENZIR_ENDPOINT to $DESTINATION_TENZIR_ENDPOINT every $S seconds"

if [ -e $TS_FILE ]; then
  PREVIOUS_TIMESTAMP=$(cat $TS_FILE)
  >&2 echo "reading start timestamp from file: $PREVIOUS_TIMESTAMP"
else
  PREVIOUS_TIMESTAMP="1970-01-01T00:00:00+00:00"
  >&2 echo "no previous sync time stamp starting from $PREVIOUS_TIMESTAMP"
fi

while true; do
  TIMESTAMP=$(date -Iseconds)
  if [ -z $Q ]; then
    QUERY="#import_time >= $PREVIOUS_TIMESTAMP && #import_time < $TIMESTAMP"
  else
    QUERY="$Q && #import_time >= $PREVIOUS_TIMESTAMP && #import_time < $TIMESTAMP"
  fi

  >&2 echo " processing time range: $PREVIOUS_TIMESTAMP <= #import_time < $TIMESTAMP"

  # count events - only run sync when count != 0
  EVENTS=$($TENZIR_BINARY -e $SOURCE_TENZIR_ENDPOINT count --estimate "$QUERY")
  >&2 echo "events to sync: $EVENTS"
  if [ $EVENTS -ne 0 ]; then
    $TENZIR_BINARY -e $SOURCE_TENZIR_ENDPOINT export arrow "$QUERY" |
      $TENZIR_BINARY -e $DESTINATION_TENZIR_ENDPOINT import arrow
    status=$?
    if [ $status -eq 0 ]; then
      >&2 echo "import succeeded"
      >&2 echo $TIMESTAMP >$TS_FILE
      PREVIOUS_TIMESTAMP=$TIMESTAMP
    else
      >&2 echo "import failed (code: $status)"
    fi
  fi
  sleep $S
done
