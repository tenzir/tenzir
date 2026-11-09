#!/bin/sh
# runner: shell
set -eu

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

cli lpush queue in-db-0 >/dev/null
cli -n 2 lpush queue in-db-2 >/dev/null
cli -n 3 lpush queue in-db-3 >/dev/null
