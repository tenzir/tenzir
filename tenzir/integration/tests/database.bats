# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  # Override parts of the default node config for this test file
  # by using `setup_node()`.
  export_default_node_config
  export TENZIR_ENABLE_METRICS=true
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

# bats test_tags=metrics
@test "batch size" {
  # Check that the import command doesn't deadlock when using the `--batch-size` option
  # and that the node keeps producing metrics.

  check tenzir "load file $INPUTSDIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"

  check --sort tenzir 'export | where resp_h == 192.168.1.104 | write ssv'

  # import some more to make sure accounting data is in the system.
  check -c \
    "gunzip -c \"$INPUTSDIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=1 -n 242 zeek"

  check --sort tenzir-ctl export json 'where resp_h == 192.168.1.104'

  # Verify that the metrics file eventually exists and contains valid JSON.
  # Chop off last line to avoid partial data.
  wait_for_file "${TENZIR_METRICS__FILE_SINK__PATH}"
  head -n -1 "${TENZIR_METRICS__FILE_SINK__PATH}" | jq
}
