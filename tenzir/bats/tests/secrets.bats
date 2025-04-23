# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export_default_node_config
  export TENZIR_SECRETS__TEST_STRING="test_value"
  export TENZIR_SECRETS__TEST_ENCODED="dGVzdF92YWx1ZQ=="
  setup_node
}

teardown() {
  teardown_node
}

@test "secret resolution" {
  check tenzir 'from { } | secret::_testing_operator secret="test_value", expected="test_value"'
  check tenzir 'secret::_testing_operator secret=secret("test-string"), expected="test_value"'
  check tenzir 'secret::_testing_operator secret=secret("test-encoded").decode_base64(), expected="test_value".encode_base64().decode_base64()'
}
