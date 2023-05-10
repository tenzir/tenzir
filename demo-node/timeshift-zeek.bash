#!/usr/bin/env bash

set -euo pipefail

input="$1"

dbg() {
  #echo "$input: ${*}" >&2
  :
}

print_event() {
  printf '%s\t%s\n' "$1" "${2:18}"
}

now=$(date '+%s.%N')
now=${now::-3}
lastts=""
cat "$input" | while read -r line; do
  if [[ "$line" == \#* ]]; then
    printf '%s\n' "$line"
    continue
  fi
  ts=$(cut -f 1 <<< "$line")
  if [ "$lastts" = "" ]; then
    firstts="$ts"
    print_event "$now" "$line"
  else
    delta=$(bc <<< "$ts - $lastts")
    newts=$(bc <<< "scale=6; $ts - $firstts + $now")
    dbg "delta = $delta"
    if [[ "$delta" = -* ]]; then
      print_event "$newts" "$line"
      continue
    fi
    # Should really be sleep_until $newts, but that is complicated.
    sleep "$delta"
    print_event "$newts" "$line"
  fi 
  lastts="$ts";
done
