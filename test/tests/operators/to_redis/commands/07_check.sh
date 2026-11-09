#!/bin/sh
# runner: shell
set -eu

# REDIS_CLI holds a command line, such as `redis-cli -p 6379`.
cli() {
  # shellcheck disable=SC2086
  $REDIS_CLI "$@"
}

echo "jobs:"
cli lrange jobs 0 -1
cli ttl jobs
echo "status:"
cli get status:web-1
cli get status:web-2
[ "$(cli ttl status:web-1)" -gt 0 ] && echo "has ttl"
echo "seen:"
cli smembers seen | sort
echo "scores:"
cli zrange scores 0 -1 withscores
echo "empty key:"
cli smembers ""
[ "$(cli ttl "")" -gt 0 ] && echo "has ttl"
echo "user:alice:"
cli -n 1 hgetall user:alice
[ "$(cli -n 1 ttl user:alice)" -gt 0 ] && echo "has ttl"
