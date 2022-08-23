#!/bin/sh
#
# A script to calculate the percent used of the filesystem that contains the
# given file paths in input order.

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <path>"
  exit 1
fi

df -P "$@" | awk '(NR>1){print $5}' | tr -d \%
