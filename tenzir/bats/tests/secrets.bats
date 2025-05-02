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
  check tenzir 'secret::_testing_operator secret="test_value", expected="test_value"'
  check tenzir 'secret::_testing_operator secret=secret("test-string"), expected="test_value"'
  check tenzir 'secret::_testing_operator secret=secret("test-string").encode_base64().decode_base64(), expected="test_value"'
  check tenzir 'secret::_testing_operator secret=secret("test-encoded").decode_base64(), expected="test_value".encode_base64().decode_base64()'
  check tenzir 'secret::_testing_operator secret=secret::_from_string("a")+"b", expected="ab"'
  check tenzir 'secret::_testing_operator secret="a"+secret::_from_string("b"), expected="ab"'
  check tenzir 'secret::_testing_operator secret=secret::_from_string("a")+secret::_from_string("b"), expected="ab"'
  check tenzir 'secret::_testing_operator secret=secret::_from_string("a")+secret::_from_string("b") + "c", expected="abc"'
  check tenzir 'secret::_testing_operator secret=secret("test-encoded").decode_base64() + "_extra", expected="test_value_extra"'
}
