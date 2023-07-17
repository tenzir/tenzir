# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=120}"

# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
export TENZIR_BARE=1

DATADIR="$BATS_SUITE_DIRNAME/../data"

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
    check tenzir-ctl import -b suricata

  check tenzir-ctl count
}

@test "parallel imports" {
  # The imports arrays hold pids of import client processes so we can wait for
  # them at any point.
  local suri_imports=()
  local zeek_imports=()
  # The `check' function must be called with -c "pipe | line" for shell pipes.
  check --bg zeek_imports -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b zeek"
  # Simple input redirection can be done by wrapping the full invocation with
  # curly braces.
  { check --bg suri_imports \
    tenzir-ctl import -b suricata; \
  } < "$DATADIR/suricata/eve.json"
  # We can also use `import -r` in this case.
  check --bg suri_imports \
    tenzir-ctl import -b -r "$DATADIR/suricata/eve.json" suricata
  check --bg suri_imports \
    tenzir-ctl import -b -r "$DATADIR/suricata/eve.json" suricata
  check --bg zeek_imports -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b zeek"
  check --bg suri_imports \
    tenzir-ctl import -b -r "$DATADIR/suricata/eve.json" suricata
  # Now we can block until all suricata ingests are finished.
  wait_all "${suri_imports[@]}"
  debug 1 "suri imports"
  check tenzir-ctl count '#schema == /suricata.*/'
  # And now we wait for the zeek imports.
  wait_all "${zeek_imports[@]}"
  debug 1 "zeek imports"
  check tenzir-ctl count '#schema == "zeek.conn"'
  check tenzir-ctl count
}

@test "batch size" {
  check -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b zeek"

  check tenzir-ctl export ascii 'resp_h == 192.168.1.104'

  # import some more to make sure accounting data is in the system.
  check -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=10 zeek"
  check -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=1000 zeek"
  check -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
     | tenzir-ctl import -b --batch-size=100000 zeek"
  check -c \
    "zcat \"$DATADIR/zeek/conn.log.gz\" \
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
