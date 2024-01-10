: "${BATS_TEST_TIMEOUT:=30}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that were spawning
# nodes with custom fixtures that may conflict with the
# default settings.

INPUTSDIR="$(dirname "$BATS_SUITE_DIRNAME")/data/inputs"
QUERYDIR="$(dirname "$BATS_SUITE_DIRNAME")/data/queries"
MISCDIR="$(dirname "$BATS_SUITE_DIRNAME")/data/misc"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  # default 'tenzir.yaml'
  export TENZIR_EXEC__DUMP_DIAGNOSTICS=true
  export TENZIR_EXPORT__ZEEK__DISABLE_TIMESTAMP_TAGS=true
  export TENZIR_AUTOMATIC_REBUILD=0
  export TENZIR_ENDPOINT=127.0.0.1:5158
  export TENZIR_PLUGINS=
  set | grep -Ee "^TENZIR" || true >&3
}

# -- tests

# bats test_tags=import,export,zeek
@test "Example config file" {
  EXAMPLE_CONFIG=${BATS_SUITE_DIRNAME}/../../../tenzir.yaml.example

  export TENZIR_DB_DIRECTORY="${BATS_TEST_TMPDIR}/db"
  # The inner exec is needed so that signals to $NODE_PID actually reach the
  # node.
  exec {NODE_OUT}< <(exec tenzir-node -e ":0" --print-endpoint)
  NODE_PID=$!
  read -r -u "$NODE_OUT" TENZIR_ENDPOINT
  export TENZIR_ENDPOINT

  tenzir --config=${EXAMPLE_CONFIG} \
    "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | import"

  check tenzir --config=${EXAMPLE_CONFIG} \
      'export | where net.app !in ["dns", "ftp", "http", "ssl"]'

  kill $NODE_PID
  wait $NODE_PID
}

# bats test_tags=metrics
@test "Shutdown with Metrics" {
  # Verify that the node doesn't deadlock on
  # shutdown when metrics are enabled.

  export TENZIR_ENABLE_METRICS=true
  setup_node

  # Random command to ensure the node is up.
  tenzir 'show serves | discard'

  teardown_node
}

# bats test_tags=disk-monitor
@test "Disk monitor" {
  # Configure the node to run every second and to delete all
  # partitions on disk. We first import the complete zeek dataset with 8462
  # events, and then wait some to give the disk monitor enough time to run.
  # After that, all events from the first import should have been erased.

  export TENZIR_MAX_PARTITION_SIZE=8462
  export TENZIR_START__DISK_BUDGET_HIGH=1
  export TENZIR_START__DISK_BUDGET_LOW=0
  export TENZIR_START__DISK_BUDGET_CHECK_INTERVAL=2
  export TENZIR_START__DISK_BUDGET_CHECK_BINARY=${MISCDIR}/scripts/count_partitions_plus1.sh

  setup_node

  import_zeek_conn
  sleep 4
  check tenzir 'export | where #schema == /zeek.*/ | summarize count=count(.)'

  teardown_node
}
