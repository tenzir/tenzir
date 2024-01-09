: "${BATS_TEST_TIMEOUT:=120}"

# This file contains the bats ports of our old integration test suite.

# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
# TODO: Should this move into `bats-tenzir`?
export TENZIR_BARE_MODE=1

# TODO: these should probably move to `test-data/input` and `test-data/queries`
DATADIR="$(dirname "$BATS_SUITE_DIRNAME")/data"
QUERYDIR="$(dirname "$BATS_SUITE_DIRNAME")/queries"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  setup_state_dir
  # default 'tenzir.yaml'
  export TENZIR_EXEC__DUMP_DIAGNOSTICS=true
  export TENZIR_EXPORT__ZEEK__DISABLE_TIMESTAMP_TAGS=true
  export TENZIR_AUTOMATIC_REBUILD=0
  export TENZIR_ENDPOINT=127.0.0.1:5158
  export TENZIR_PLUGINS=
  # bats settings
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


# bats test_tags=node,counting,zeek
@test "conn log counting" {
  check -c "zcat ${DATADIR}/zeek/conn.log.gz | tenzir-ctl -N --max-partition-size=64 import zeek"
  run tenzir-ctl flush
  check tenzir-ctl -N count ":ip == 192.168.1.104"
  check tenzir-ctl -N count -e ":ip == 192.168.1.104"
  check tenzir-ctl -N count "resp_p == 80"
  check tenzir-ctl -N count "resp_p != 80"
  check tenzir-ctl -N count 861237
}


# bats test_tags=node,import,export,zeek
@test "node zeek conn log" {
    check -c "zcat data/zeek/conn.log.gz | tenzir-ctl -N import zeek"
    check tenzir-ctl -N export ascii 'where resp_h == 192.168.1.104'
    check tenzir-ctl -N export ascii 'where orig_bytes > 1k && orig_bytes < 1Ki'
    check tenzir-ctl -N export ascii 'where :string == "OrfTtuI5G4e" || :port == 67 || :uint64 == 67'
    check tenzir-ctl -N export ascii "where #schema == \"zeek.conn\" && resp_h == 192.168.1.104"
    check tenzir-ctl -N export ascii "where #schema != \"zeek.conn\" && #schema != \"tenzir.metrics\""
    check tenzir-ctl -N export ascii "where #schema != \"foobar\" && resp_h == 192.168.1.104"
}
