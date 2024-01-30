# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "invalid fluent-bit plugin" {
  check ! tenzir 'fluent-bit please-do-not-crash'
}

@test "infer schema from random data" {
  check tenzir 'fluent-bit random | head 1 | put schema=#schema'
}

@test "read stdin via fluent-bit" {
  echo '{"foo": {"bar": 42}}' | check tenzir 'fluent-bit stdin | drop timestamp'
}

@test "fluent-bit works as a sink" {
  check tenzir 'version | fluent-bit null'
}

@test "Use fluent-bit to count to 10" {
  run -0 --separate-stderr \
    tenzir 'version | repeat | head | fluent-bit counter'
  { check cut -d , -f 2; } <<<"$output"
}
