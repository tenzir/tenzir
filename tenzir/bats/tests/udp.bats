: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "loader - listen" {
  check --bg listen \
    tenzir 'from udp://127.0.0.1:56789 | head 1'
  timeout 10 bash -c 'until lsof -i :56789; do sleep 0.2; done'
  jq -n '{foo: 42}' | socat - udp-send:127.0.0.1:56789
  wait_all "${listen[@]}"
}

@test "saver" {
  check --bg listen \
    tenzir 'from udp://127.0.0.1:55555 | head 10'
  timeout 10 bash -c 'until lsof -i :55555; do sleep 0.2; done'
  tenzir 'version | put foo=42 | repeat 10 | to udp://127.0.0.1:55555'
  wait_all "${listen[@]}"
}

@test "saver - message too long" {
  printf "%65535s" | check tenzir 'save udp 127.0.0.1:54321' || true
}
