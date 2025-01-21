: "${BATS_TEST_TIMEOUT:=30}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that were spawning
# nodes with custom fixtures that may conflict with the
# default settings.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export_default_node_config
}

# -- tests

# bats test_tags=import,export,zeek
@test "Example config file" {
  EXAMPLE_CONFIG=${BATS_TEST_DIRNAME}/../../../tenzir.yaml.example

  setup_node --config=${EXAMPLE_CONFIG}

  tenzir --config=${EXAMPLE_CONFIG} \
    "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | import"

  check tenzir --config=${EXAMPLE_CONFIG} \
    'export | where net.app !in ["dns", "ftp", "http", "ssl"]'

  teardown_node
}

# bats test_tags=metrics,flaky
@test "Shutdown with Metrics" {
  skip "Disabled due to CI flakiness"

  # Verify that the node doesn't deadlock on
  # shutdown when metrics are enabled.

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
  check tenzir 'partitions | where schema == /zeek.*/ | summarize count=count(.)'

  teardown_node
}
