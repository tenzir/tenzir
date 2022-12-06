#!/usr/bin/env bash

# Synchonizes two VAST instances 
# uses`vast -e <source-vast> export arrow | vast -e <destionation-vast> import arrow`
# repeatedly (every `k` seconds)
# stores the last successful sync ID in a file and resumes from that timestamp if
# the file already exists:
TS_FILE="last_queried.ts"
VAST_BINARY="${1:-,vast}"
S=${2:-1}
SOURCE_VAST_ENDPOINT=${3:-:42000}
DESTINATION_VAST_ENDPOINT=${4:-:42001}

echo "syncing $SOURCE_VAST_ENDPOINT to $DESTINATION_VAST_ENDPOINT every $S seconds"

TS_FROM_FILE=`cat $TS_FILE`
PREVIOUS_TIMESTAMP=${TS_FROM_FILE:-1970-01-01T00:00:00+00:00}

echo resuming sync at: $PREVIOUS_TIMESTAMP

while true; do
  TIMESTAMP=`date -Iseconds`
  QUERY="#type == \"suricata.alert\" && #import_time > $PREVIOUS_TIMESTAMP && #import_time <= $TIMESTAMP"

  echo " processing time range: $PREVIOUS_TIMESTAMP < #import_time <= $TIMESTAMP"

  # count events - only run sync when count != 0
  EVENTS=`$VAST_BINARY -e $SOURCE_VAST_ENDPOINT count "$QUERY"`
  echo "events to sync: $EVENTS"
  if [ $EVENTS -ne 0 ]
  then
    $VAST_BINARY -e $SOURCE_VAST_ENDPOINT export arrow "$QUERY" \
      |  $VAST_BINARY -e $DESTINATION_VAST_ENDPOINT import arrow
    status=$?
    if [ $status -eq 0 ]
    then
      echo "import succeeded"
      echo $TIMESTAMP > $TS_FILE
      PREVIOUS_TIMESTAMP=$TIMESTAMP
    else
      echo "import failed (code: $status)"
    fi
  fi
  sleep $S
done
