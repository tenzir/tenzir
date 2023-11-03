# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

RULE="${BATS_SUITE_DIRNAME}/test.yara"
COMPILED_RULE="${BATS_TEST_DIRNAME}/test.yarac"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "produce events for matching strings" {
  echo 'foo bar baz' | check tenzir "load stdin | yara ${RULE}"
}

@test "remain silent for true negatives" {
  echo 'foo' | check tenzir "load stdin | yara ${RULE}"
}

@test "work with a compiled rule" {
  yarac "${RULE}" "${COMPILED_RULE}"
  echo 'baz' | check tenzir "load stdin | yara -C ${COMPILED_RULE}"
}

@test "match blockwise per chunk" {
  echo 'foo bar' | check tenzir "load stdin | repeat 2 | yara -B ${RULE}"
}

@test "accumulate chunks and match when the input exhausted" {
  echo 'foo bar' | check tenzir "load stdin | repeat 2 | yara ${RULE}"
}
