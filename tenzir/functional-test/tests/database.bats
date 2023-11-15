# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
export TENZIR_BARE_MODE=1

DATADIR="$(dirname "$BATS_SUITE_DIRNAME")/data"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  setup_state_dir
  export TENZIR_AUTOMATIC_REBUILD=0
  export TENZIR_ENABLE_METRICS=true
  export TENZIR_METRICS__SELF_SINK__ENABLE=false
  export TENZIR_METRICS__FILE_SINK__ENABLE=true
  export TENZIR_METRICS__FILE_SINK__REAL_TIME=true
  export TENZIR_METRICS__FILE_SINK__PATH=$PWD/$BATS_TEST_STATE_DIR/metrics.log
  set | grep -Ee "^TENZIR" || true >&3
  setup_node
}

teardown() {
  teardown_node
  teardown_state_dir
}

@test "import and export commands" {
  < "$DATADIR/suricata/eve.json" \
    check tenzir 'read suricata | import'

  check tenzir-ctl count
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
#     "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
#      | tenzir 'read zeek-tsv | import'"
#   # Simple input redirection can be done by wrapping the full invocation with
#   # curly braces.
#   { check --bg suri_imports \
#     tenzir 'read suricata | import'; \
#   } < "$DATADIR/suricata/eve.json"
#   # We can also use `import -r` in this case.
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
#   check --bg zeek_imports \
#     tenzir "load file $DATADIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
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
# =======
# @test "parallel imports" {
#   # The imports arrays hold pids of import client processes so we can wait for
#   # them at any point.
#   local suri_imports=()
#   local zeek_imports=()
#   # The `check' function must be called with -c "pipe | line" for shell pipes.
#   # Note that we will use the decompress operator in other places, this is just
#   # an exposition.
#   check --bg zeek_imports -c \
#     "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
#      | tenzir 'read zeek-tsv | import'"
#   # Simple input redirection can be done by wrapping the full invocation with
#   # curly braces.
#   { check --bg suri_imports \
#     tenzir 'read suricata | import'; \
#   } < "$DATADIR/suricata/eve.json"
#   # We can also use `import -r` in this case.
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
#   check --bg zeek_imports \
#     tenzir "load file $DATADIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"
#   check --bg suri_imports \
#     tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
#   # Now we can block until all suricata ingests are finished.
#   wait_all "${suri_imports[@]}"
#   debug 1 "suri imports"
#   check tenzir-ctl count '#schema == /suricata.*/'
#   # And now we wait for the zeek imports.
#   wait_all "${zeek_imports[@]}"
#   debug 1 "zeek imports"
#   check tenzir-ctl count '#schema == "zeek.conn"'
#   check tenzir-ctl count
# }

@test "batch size" {
  check tenzir "load file $DATADIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"

  check --sort tenzir 'export | where resp_h == 192.168.1.104 | write ssv'

  # import some more to make sure accounting data is in the system.
  check -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=1 -n 242 zeek"

  check -c \
    "tenzir-ctl status --detailed \
     | jq '.index.statistics.layouts | del(.\"tenzir.metrics\")'"

  check --sort -c \
    "tenzir-ctl status --detailed | jq -ec 'del(.version) | del(.system.\"swap-space-usage\") | paths(scalars) as \$p | {path:\$p, type:(getpath(\$p) | type)}' | grep -v ',[1-9][0-9]*,'"

  check -c \
    "tenzir-ctl status --detailed index importer \
     | jq -ec 'paths(scalars) as \$p | {path:\$p, type:(getpath(\$p) | type)}' | grep -v ',[1-9][0-9]*,'"

  check --sort tenzir-ctl export json 'where resp_h == 192.168.1.104'

  # FIXME: This test is currently disabled as it is flaky. Let's figure out why
  # in the future.
  # # Unfortunately necessary.
  # sleep 5
  # sync "${TENZIR_METRICS__FILE_SINK__PATH}"
  # check -c "jq -c '{key: .key, type: .value | type}' \"${TENZIR_METRICS__FILE_SINK__PATH}\" | sort | uniq"
}
