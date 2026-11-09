#!/bin/sh
# runner: shell
set -eu

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

cli lpush first x >/dev/null
cli lpush second y >/dev/null
cli lpush second z >/dev/null
