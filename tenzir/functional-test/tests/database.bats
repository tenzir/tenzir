# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  # Override parts of the default node config for this test file
  # by using `setup_node_raw()`.
  export_default_node_config
  export TENZIR_ENABLE_METRICS=true
  export TENZIR_METRICS__SELF_SINK__ENABLE=false
  export TENZIR_METRICS__FILE_SINK__ENABLE=true
  export TENZIR_METRICS__FILE_SINK__REAL_TIME=true
  export TENZIR_METRICS__FILE_SINK__PATH=${TENZIR_STATE_DIRECTORY}/metrics.log
  setup_node_raw
}

teardown() {
  teardown_node
}

@test "import and export operators" {
  check <"$INPUTSDIR/suricata/eve.json" tenzir 'read suricata | import'

  check tenzir 'export | summarize count=count(.)'
}

# TODO This test is currently disabled because it is flaky in the macOS CI.
# See tenzir/issues#995.
# @test "parallel imports" {
#   # The imports arrays hold pids of import client processes so we can wait for
#   # them at any point.
#   local suri_imports=()
#   local zeek_imports=()
#   # The `check' function must be called with -c "pipe | line" for shell pipes.
#   # Note that we will use the decompress operator in other places, this is just
#   # an exposition.
#   check --bg zeek_imports -c \
#     "gunzip -c \"$INPUTSDIR/zeek/conn.log.gz\" \
#      | tenzir 'read zeek-tsv | import'"
#   # Simple input redirection can be done by wrapping the full invocation with
#   # curly braces.
#   { check --bg suri_imports \
#     tenzir 'read suricata | import'; \
#   } < "$INPUTSDIR/suricata/eve.json"
#   # We can also use `import -r` in this case.
#   check --bg suri_imports \
#     tenzir "from file $INPUTSDIR/suricata/eve.json read suricata | import"
#   check --bg suri_imports \
#     tenzir "from file $INPUTSDIR/suricata/eve.json read suricata | import"
#   check --bg zeek_imports \
#     tenzir "load file $INPUTSDIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"
#   check --bg suri_imports \
#     tenzir "from file $INPUTSDIR/suricata/eve.json read suricata | import"
#   # Now we can block until all suricata ingests are finished.
#   wait_all "${suri_imports[@]}"
#   debug 1 "suri imports"
#   # TODO: Flushing should not be necessary!
#   tenzir-ctl flush
#   check tenzir-ctl count '#schema == /suricata.*/'
#   # And now we wait for the zeek imports.
#   wait_all "${zeek_imports[@]}"
#   tenzir-ctl flush
#   debug 1 "zeek imports"
#   check tenzir-ctl count '#schema == "zeek.conn"'
#   check tenzir-ctl count
# }

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
