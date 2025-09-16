: "${BATS_TEST_TIMEOUT:=60}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that were written
# before pipelines were introduced and are not using any
# server fixture.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  setup_node_with_default_config
  export TENZIR_LEGACY=true
}

teardown() {
  teardown_node
}

# -- Test Suites

# bats test_tags=node,import,export,zeek
@test "Node Zeek conn log" {
  import_zeek_conn

  check tenzir "export | where resp_h == 192.168.1.104 | sort ts"
  check tenzir "export | where orig_bytes > 1k and orig_bytes < 1Ki | sort ts"
  check tenzir 'export | where :string == "OrfTtuI5G4e" or :uint64 == 67 | sort ts'
  check tenzir 'export | where #schema == "zeek.conn" and resp_h == 192.168.1.104 | sort ts'
  check tenzir 'export | where #schema != "zeek.conn" | sort ts'
  check tenzir 'export | where #schema != "foobar" and resp_h == 192.168.1.104 | sort ts'
}

# bats test_tags=node,import,export,zeek
@test "Node Zeek http log" {
  import_zeek_http

  check tenzir "export | where resp_h == 216.240.189.196 | sort ts"
}

# bats test_tags=node,import,export,zeek
@test "Node Zeek snmp log" {
  import_zeek_snmp

  check tenzir "export | where duration >= 3s | sort ts"
}

# bats test_tags=node,import,export,zeek,json
@test "Node Zeek JSON" {
  import_zeek_json

  check --sort -c "tenzir 'export | where \"zeek\" in #schema' | jq --sort-keys -ec ."
}

# bats test_tags=node,type
@test "Node type query" {
  import_zeek_conn "head 20"

  check tenzir "export | sort ts"
}

#bats test_tags=node,import,export,suricata,eve,import_filter
@test "Node suricata alert" {
  import_suricata_eve 'where #schema != "suricata.stats" and event_type != "flow"'

  check tenzir "export | where src_ip == 147.32.84.165 | sort timestamp"
  check tenzir "export | where #schema == /suricata.alert/ | sort timestamp | write tsv"
  check tenzir "export | sort timestamp | write json --omit-nulls --omit-empty-objects"
}

#bats test_tags=node,import,export,suricata,eve
@test "Node suricata rrdata" {
  import_data "load file ${INPUTSDIR}/suricata/rrdata-eve.json | read json --selector=event_type:suricata"

  check tenzir 'export | sort timestamp'
  check tenzir 'export | where #schema == "suricata.dns" | sort timestamp'
}

#bats test_tags=import,export,zeek
@test "multi addr query" {
  import_zeek_conn

  query=$(<${QUERYDIR}/multi_addr.txt)
  check tenzir "export | ${query}"
}

#bats test_tags=syslog,import
@test "Import syslog" {
  import_data "from ${INPUTSDIR}/syslog/syslog.log read syslog"

  check --sort -c "tenzir 'export | where #schema == /syslog.*/' | jq --sort-keys -ec ."
}

# TODO: Figure out why this one is flaky in CI
#bats test_tags=import,json,sysmon,flaky
@test "Heterogeneous jsonl import" {
  skip "Temporarily disabled due to CI flakinesss"

  import_suricata_eve
  import_data "from ${INPUTSDIR}/json/sysmon.json"

  check --sort -c "tenzir 'export | where \"®\" in :string' | jq --sort-keys -ec ."
  check --sort -c "tenzir 'export | where #schema ni \"suricata\"' | jq --sort-keys -ec ."

  check tenzir 'export | where ProcessGuid == /\{[0-9a-f]{8}-[0-9a-f]{4}-5ec2-7.15-[0-9a-f]{12}\}/ | sort UtcTime | sort ProcessId'
}

#bats test_tags=import,json,suricata
@test "Import time" {
  import_suricata_eve 'where #schema == "suricata.stats"'

  if [ $(uname) == "Darwin" ]; then
    if [ ! command -v gdate ]; then
      skip "this test requires coreutils to be installed on macOS"
    fi
    # We need subsecond precision here because `date` rounds down otherwise.
    NOW=$(gdate -Ins)
  else
    NOW=$(date -Ins)
  fi

  check tenzir "export | where #import_time > ${NOW}"
  check tenzir "export | where #import_time <= ${NOW} | sort timestamp"
  check tenzir "export | where #import_time > now"
  check tenzir "export | where #import_time <= now | sort timestamp"
}

#bats test_tags=import,export,suricata
@test "Extractor Predicates" {
  import_suricata_eve

  check tenzir "export | where timestamp and :ip | sort timestamp"
  check tenzir "export | where does_not_exist | sort timestamp"
  check tenzir "export | where flow.alerted | sort timestamp"
}

#bats test_tags=import,json
@test "Nested Records" {
  import_data "from ${INPUTSDIR}/json/record-in-list.json"

  check --sort tenzir "export"
}

# bats test_tags=import,export,pipelines,chart,line-chart
@test "Line chart" {
  import_zeek_conn

  check tenzir "export | head 10 | sort ts asc | select ts, orig_bytes | chart line"
  check tenzir "export | head 10 | sort ts asc | select ts, orig_bytes | chart line | get-attributes"
  check tenzir "export | head 10 | sort ts asc | chart line -x=ts -y=orig_bytes"
  check tenzir "export | head 10 | sort ts asc | chart line -x=ts -y=orig_bytes | get-attributes"
  check ! tenzir "export | head 10 | sort ts desc | chart line -x=ts -y=orig_bytes"
}
