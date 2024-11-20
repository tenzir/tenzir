# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  # Override parts of the default node config for this test file
  # by using `setup_node()`.
  export_default_node_config
  export TENZIR_METRICS__SELF_SINK__ENABLE=false
  export TENZIR_METRICS__FILE_SINK__ENABLE=true
  export TENZIR_METRICS__FILE_SINK__REAL_TIME=true
  export TENZIR_METRICS__FILE_SINK__PATH=${TENZIR_STATE_DIRECTORY}/metrics.log
  setup_node
}

teardown() {
  teardown_node
}

@test "import and export operators" {
  check <"$INPUTSDIR/suricata/eve.json" tenzir 'read suricata | import'

  check tenzir 'export | summarize count=count(.)'
}

wait_for_file() {
  local file=$1
  until [[ -s "$file" ]]; do
    sleep 1
  done
}
