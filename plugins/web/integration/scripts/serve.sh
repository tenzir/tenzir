#! /usr/bin/env bash

set -euxo pipefail

"${app}" --bare-mode --plugins=web --endpoint=127.0.0.1:42024 exec 'version | repeat 5 | serve version' &

sleep 3

# Query the first set.
first_result="$(curl -XPOST -H "Content-Type: application/json" -d '{"serve_id": "version", "timeout": "5s", "max_events": 1}' http://127.0.0.1:42025/api/v0/serve)"
continuation_token="$(jq -rnec "${first_result} | .next_continuation_token")"

# The first set should be 1 results exactly.
jq -nec "${first_result} | .events | length == 1"

# Query the second set.
second_result="$(curl -XPOST -H "Content-Type: application/json" -d "{\"serve_id\": \"version\", \"continuation_token\": \"${continuation_token}\", \"timeout\": \"5s\", \"max_events\": 4}" http://127.0.0.1:42025/api/v0/serve)"

# The next set should be the reamining 4 results.
jq -nec "${second_result} | .next_continuation_token == null"
jq -nec "${second_result} | .events | length == 4"

wait
