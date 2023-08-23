#! /usr/bin/env bash

set -euxo pipefail

"${app}" --bare-mode --plugins=web --endpoint=127.0.0.1:42024 exec 'show version | repeat 5 | serve version' &

sleep 3

num_results=0

# Query the first set.
response="$(curl -XPOST -H "Content-Type: application/json" -d '{"serve_id": "version", "timeout": "5s", "max_events": 1, "continuation_token": null}' http://127.0.0.1:42025/api/v0/serve)"
num_results="$(( $num_results + $(jq -nec "${response} | .events | length") ))"

while jq -rnec "${response} | .next_continuation_token != null" > /dev/null; do
  response="$(curl -XPOST -H "Content-Type: application/json" -d "{\"serve_id\": \"version\", \"continuation_token\": \"$(jq -rnec "${response} | .next_continuation_token")\", \"timeout\": \"5s\", \"max_events\": 4}" http://127.0.0.1:42025/api/v0/serve)"
  num_results="$(( $num_results + $(jq -nec "${response} | .events | length") ))"
done

# There must be a total of 5 results.
echo "$num_results"

wait
