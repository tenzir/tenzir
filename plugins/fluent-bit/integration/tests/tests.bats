# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export_default_node_config
  export TENZIR_PLUGINS="fluent-bit"
}

@test "invalid fluent-bit plugin" {
  check ! tenzir 'fluent-bit please-do-not-crash'
}

@test "infer schema from random data" {
  check tenzir 'fluent-bit random | head 1 | put schema=#schema'
}

@test "read stdin via fluent-bit" {
  # In general we deem the fluent-bit stdin functionality as too unreliable.
  # Please refrain if you came here with the intention to re-enable this test,
  # we only keep it to document that the operator should not be tested this
  # way.
  skip "This test often fails to produce any output when run on GitHub Actions Runners"
  # fluent-bit has an internal race condition that can cause the loss of
  # events during startup. We have to accept this for now and avoid
  # test flakyness with a bit of trickery.
  result="$(
    while :; do
      echo '{"foo": {"bar": 42}}'
      sleep 1
    done || exit 1 |
      tenzir 'fluent-bit stdin | drop timestamp | head 1' |
      grep -v 'kevent'
  )"
  check echo "$result"
}

@test "fluent-bit works as a sink" {
  check tenzir 'version | fluent-bit null'
}

@test "Use fluent-bit to count to 10" {
  skip "Disabled due to CI flakiness"
  run -0 --separate-stderr \
    tenzir 'version | repeat | head | fluent-bit counter'
  { check cut -d , -f 2; } <<<"$output"
}
