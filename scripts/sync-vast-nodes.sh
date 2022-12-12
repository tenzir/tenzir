#!/usr/bin/env bash

# Synchonizes two VAST instances 
# uses`vast -e <source-vast> export arrow | vast -e <destionation-vast> import arrow`
# repeatedly (every `k` seconds)
# stores the last successful sync timestamp in a file and resumes from that timestamp if
# the file already exists, otherwise it syncs all previous events.

TS_FILE="last_queried.ts"

Q=${1:-""}
S=${2:-5}
SOURCE_VAST_ENDPOINT=${3:-:42000}
DESTINATION_VAST_ENDPOINT=${4:-:42001}
VAST_BINARY="${5:-vast}"

echo "syncing $SOURCE_VAST_ENDPOINT to $DESTINATION_VAST_ENDPOINT every $S seconds"

if [ -e $TS_FILE ]
then
  PREVIOUS_TIMESTAMP=`cat $TS_FILE`
  echo "reading start timestamp from file: $PREVIOUS_TIMESTAMP"
else
  PREVIOUS_TIMESTAMP="1970-01-01T00:00:00+00:00"
  echo "no previous sync time stamp starting from $PREVIOUS_TIMESTAMP"
fi 

while true; do
  TIMESTAMP=`date -Iseconds`
  if [ -z $Q ]
  then
    QUERY="#import_time > $PREVIOUS_TIMESTAMP && #import_time <= $TIMESTAMP"
  else
    QUERY="$Q && #import_time > $PREVIOUS_TIMESTAMP && #import_time <= $TIMESTAMP"
  fi

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
