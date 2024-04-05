: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "remote operators shut down with node" {
  setup_node_with_default_config
  check ! --bg client tenzir 'export --live'
  # HACK: We run something else that goes through the node actor, just so that
  # we can be sure that the remote operator was actually spawned from the
  # background process.
  check tenzir 'api /ping | discard'
  teardown_node
  wait "${client[@]}"
}
