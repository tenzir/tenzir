: "${BATS_TEST_TIMEOUT:=120}"

# This file contains the bats ports of tests related to specific operators.

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

# bats test_tags=parser
@test "basic command-line parsing" {
    tenzir --dump-ast " "
    tenzir --dump-ast "// comment"
    tenzir --dump-ast "#!/usr/bin/env tenzir"
}

# bats test_tags=parser
@test "operator parsing" {
    check tenzir --dump-ast "version"
    check ! tenzir "version --tev"
    check ! tenzir "version 42"
    check ! tenzir "from a/b/c.json read json"
    check tenzir --dump-ast "from file a/b/c.json"
    check tenzir --dump-ast "from file a/b/c.json read cef"
    check tenzir --dump-ast "read zeek-tsv"
    check tenzir --dump-ast "head 42"
    check tenzir --dump-ast "local remote local pass"
    check tenzir --dump-ast "where :ip == 1.2.3.4"
    check tenzir --dump-ast "to file xyz.json"
    check tenzir "from ${DATADIR}/json/all-types.json read json"
    check tenzir "from file://${DATADIR}/json/all-types.json read json"
    check ! tenzir "from file:///foo.json read json"
    check ! tenzir "from scheme://foo.json read json"
    check ! tenzir "from scheme:foo.json read json"
    check ! tenzir "load file foo.json | read json"
    check ! tenzir "load file | read json"
    check ! tenzir "load ./file | read json"
    check ! tenzir "load filee | read json"
    check tenzir "from ${DATADIR}/json/basic-types.json"
    check tenzir "from ${DATADIR}/json/dns.log.json.gz | head 2"
    check tenzir "from ${DATADIR}/json/dns.log.json.gz read json | head 2"
    check tenzir "from ${DATADIR}/suricata/eve.json | head 2"
    check tenzir "from ${DATADIR}/zeek/http.log.gz read zeek-tsv | head 2"
    check tenzir --dump-ast "from a/b/c.json | where xyz == 123 | to foo.csv"
}


# bats test_tags=pipelines
@test "apply operator" {
      check tenzir "apply ${QUERYDIR}/some_source | write json"
      check tenzir "apply ${QUERYDIR}/some_source.tql | write json"
      check ! tenzir "apply /tmp/does_not_exist"
      check ! tenzir "apply does_not_exist.tql"
      run ! tenzir "apply ${QUERYDIR}/from_unknown_file.tql"
}
