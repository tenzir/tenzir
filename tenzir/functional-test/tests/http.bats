: "${BATS_TEST_TIMEOUT:=10}"

export TENZIR_PLUGINS="fluent-bit"

wait_for_tcp() {
  port=$1
  timeout $BATS_TEST_TIMEOUT bash -c "until echo > /dev/tcp/127.0.0.1/$port; do sleep 0.2; done"
}

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "saver" {
  # TODO: Write convenience function to get current tenzir config
  # TODO: Why is this test even here and not in the fluent-bit plugin?
  setup_node
  has_fluentbit=$(tenzir 'show plugins | where name == "fluent-bit"')
  teardown_node

  if [ -z $has_fluentbit ]; then
    skip "built without fluent-bit support"
  fi

  listen=()
  check ! --bg listen \
    tenzir 'fluent-bit http port=8888 | yield message'
  wait_for_tcp 8888

  run tenzir 'version | put foo="bar" | to http://127.0.0.1:8888/'

  # We need to wait some until the background pipeline writes its data.
  sleep 5
  # TODO: Generalize and move to bats-tenzir.
  pkill -P $(pgrep -P "${listen[0]}") tenzir
  wait_all "${listen[@]}"
}
