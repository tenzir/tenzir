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
  setup_node_with_default_config

  # Our `http` connector is just a client, and not a server. Hence we're relying
  # on Fluent Bit here because it provides us with a web server that we can test
  # against.
  has_fluentbit=$(tenzir 'show plugins | where name == "fluent-bit"')
  teardown_node
  if [ -z $has_fluentbit ]; then
    skip "built without fluent-bit support"
  fi

  # Setup the HTTP server to test against.
  listen=()
  check ! --bg listen \
    tenzir 'fluent-bit http port=8888 | yield message'
  wait_for_tcp 8888

  # Test the `http` client connector.
  run tenzir 'version | put foo="bar" | to http://127.0.0.1:8888/'

  # We need to wait some until the background pipeline writes its data.
  sleep 5
  # TODO: Generalize and move to bats-tenzir.
  pkill -P $(pgrep -P "${listen[0]}") tenzir
  wait_all "${listen[@]}"
}
