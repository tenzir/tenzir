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
  export TENZIR_AUTOMATIC_REBUILD=0
  set | grep -Ee "^TENZIR" || true >&3
  setup_db
  setup_node
}

teardown() {
  teardown_node
  teardown_db
}

@test "import and export commands" {
  < "$DATADIR/suricata/eve.json" \
    check tenzir 'read suricata | import'
  # TODO: Flushing should not be necessary!
  tenzir-ctl flush

  check tenzir-ctl count
}

@test "parallel imports" {
  # The imports arrays hold pids of import client processes so we can wait for
  # them at any point.
  local suri_imports=()
  local zeek_imports=()
  # The `check' function must be called with -c "pipe | line" for shell pipes.
  # Note that we will use the decompress operator in other places, this is just
  # an exposition.
  check --bg zeek_imports -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir 'read zeek-tsv | import'"
  # Simple input redirection can be done by wrapping the full invocation with
  # curly braces.
  { check --bg suri_imports \
    tenzir 'read suricata | import'; \
  } < "$DATADIR/suricata/eve.json"
  # We can also use `import -r` in this case.
  check --bg suri_imports \
    tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
  check --bg suri_imports \
    tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
  check --bg zeek_imports \
    tenzir "load file $DATADIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"
  check --bg suri_imports \
    tenzir "from file $DATADIR/suricata/eve.json read suricata | import"
  # Now we can block until all suricata ingests are finished.
  wait_all "${suri_imports[@]}"
  debug 1 "suri imports"
  # TODO: Flushing should not be necessary!
  tenzir-ctl flush
  check tenzir-ctl count '#schema == /suricata.*/'
  # And now we wait for the zeek imports.
  wait_all "${zeek_imports[@]}"
  tenzir-ctl flush
  debug 1 "zeek imports"
  check tenzir-ctl count '#schema == "zeek.conn"'
  check tenzir-ctl count
}

@test "batch size" {
  check tenzir "load file $DATADIR/zeek/conn.log.gz | decompress gzip | read zeek-tsv | import"
  tenzir-ctl flush

  check tenzir 'export | where resp_h == 192.168.1.104 | write ssv'

  # import some more to make sure accounting data is in the system.
  check -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=10 zeek"
  check -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=1000 zeek"
  check -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=100000 zeek"
  check -c \
    "gunzip -c \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=1 -n 242 zeek"

  check -c \
    "tenzir-ctl status --detailed \
     | jq '.index.statistics.layouts | del(.\"tenzir.metrics\")'"

  check -c \
    "tenzir-ctl status --detailed | jq -ec 'del(.version) | del(.system.\"swap-space-usage\") | paths(scalars) as \$p | {path:\$p, type:(getpath(\$p) | type)}' | grep -v ',[1-9][0-9]*,'"

  check -c \
    "tenzir-ctl status --detailed index importer \
     | jq -ec 'paths(scalars) as \$p | {path:\$p, type:(getpath(\$p) | type)}' | grep -v ',[1-9][0-9]*,'"
}
