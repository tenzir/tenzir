# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

RULE_DIR="${BATS_SUITE_DIRNAME}"
TMP_DIR="${BATS_TEST_DIRNAME}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "generate matches for source rule" {
  echo 'foo barbar baz' | check tenzir "load stdin | yara ${RULE_DIR}/test.yara"
}

@test "generate matches for compiled rule" {
  yarac "${RULE_DIR}/test.yara" "${TMP_DIR}/test.yarac"
  echo 'foo barbar baz' |
    check tenzir "load stdin | yara -C ${TMP_DIR}/test.yarac"
}

@test "remain silent for true negatives" {
  echo 'qux quux' | check tenzir "load stdin | yara ${RULE_DIR}/test.yara"
}

@test "match blockwise per chunk" {
  echo 'foo bar' |
    check tenzir "load stdin | repeat 2 | yara -B -C ${TMP_DIR}/test.yarac"
}

@test "match once at the end" {
  echo 'foo bar' |
    check tenzir "load stdin | repeat 2 | yara -C ${TMP_DIR}/test.yarac"
}
