# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

RULE_DIR="$BATS_SUITE_DIRNAME"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "generate matches for source rule" {
  echo 'foo barbar baz' | check tenzir "load stdin | yara ${RULE_DIR}/test.yara"
}

@test "generate matches for compiled rule" {
  yarac ${RULE_DIR}/test.yara test.yarac
  echo 'foo barbar baz' | check tenzir 'load stdin | yara -C test.yarac'
}

@test "remain silent for true negatives" {
  echo 'qux quux' | check tenzir "load stdin | yara ${RULE_DIR}/test.yara"
}
