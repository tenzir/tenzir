#!/bin/sh
# runner: shell
set -eu

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

# Only database 0 still holds its item.
cli llen queue
cli -n 2 llen queue
cli -n 3 llen queue
