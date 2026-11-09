#!/bin/sh
# runner: shell
set -eu

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

# A D4 server pushes every item to the left end of an analyzer queue.
for item in one two three four; do
  cli lpush "analyzer:8:3a7e4b8c" "$item" >/dev/null
done
