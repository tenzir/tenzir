# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  setup_state_dir
  TENZIR_START__COMMANDS="web server --mode=dev --port=5160"
  export TENZIR_START__COMMANDS
  setup_node
}

teardown() {
  teardown_node
  teardown_state_dir
}

wait_for_http() {
  timeout 10 bash -c 'until echo > /dev/tcp/127.0.0.1/5160; do sleep 0.1; done'
}

@test "serve endpoint" {
  wait_for_http

  tenzir 'show version | repeat 5 | serve version' &
  sleep 3

  # Query the first set.
  first_result="$(curl -XPOST -H "Content-Type: application/json" -d '{"serve_id": "version", "timeout": "5s", "max_events": 1}' http://127.0.0.1:5160/api/v0/serve)"
  continuation_token="$(jq -rnec "${first_result} | .next_continuation_token")"

  # The first set should be 1 results exactly.
  check jq -nec "${first_result} | .next_continuation_token != null"
  check jq -nec "${first_result} | .events | length"

  # Query the second set.
  second_result="$(curl -XPOST -H "Content-Type: application/json" -d "{\"serve_id\": \"version\", \"continuation_token\": \"${continuation_token}\", \"timeout\": \"5s\", \"max_events\": 4}" http://127.0.0.1:5160/api/v0/serve)"
  continuation_token="$(jq -rnec "${first_result} | .next_continuation_token")"

  # The next set should be the remaining 4 results.
  check jq -nec "${second_result} | .events | length"

  # Pull once more if necessary.
  if jq -nec "${continuation_token} != null"; then
    third_result="$(curl -XPOST -H "Content-Type: application/json" -d "{\"serve_id\": \"version\", \"continuation_token\": \"${continuation_token}\", \"timeout\": \"5s\", \"max_events\": 1}" http://127.0.0.1:5160/api/v0/serve)"
    assert jq -nec "${third_result} | .next_continuation_token == null"
    assert jq -nec "(${third_result} | .events | length) == 0"
  fi
}
