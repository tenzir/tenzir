: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "loader - listen" {
  check --bg listen \
    tenzir 'load_udp "127.0.0.1:59677" | read_json | head 1'
  timeout 10 bash -c 'until lsof -i :59677; do sleep 0.2; done'
  jq -n '{foo: 42}' | socat - udp-send:127.0.0.1:59677
  wait_all "${listen[@]}"
}

@test "saver" {
  check --bg listen \
    tenzir 'load_udp "127.0.0.1:58974" | read_json | head 10'
  timeout 10 bash -c 'until lsof -i :58974; do sleep 0.2; done'
  tenzir 'from {foo: 42} | repeat 10 | write_json | save_udp "127.0.0.1:58974"'
  wait_all "${listen[@]}"
}

@test "saver - message too long" {
  printf "%65535s" | check tenzir 'save_udp "127.0.0.1:54321"' || true
}
