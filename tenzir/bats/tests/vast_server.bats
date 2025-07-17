: "${BATS_TEST_TIMEOUT:=60}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that were using
# the old ServerFixture helper class.

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

# -- Tests ----------------------------------------

# bats test_tags=server,operator
@test "Query Operators" {
  import_zeek_conn

  check tenzir 'export | where conn.duration <= 1.0s | sort ts'
  check tenzir 'export | where duration >= 10.0s && duration < 15s | sort ts'
  check tenzir 'export | where duration >= 1.8s && duration < 2.0s | sort ts'
  check tenzir 'export | where service  == "smtp" | sort ts'
  check tenzir 'export | where missed_bytes  != 0 | sort ts'
  check tenzir 'export | where id.orig_h !in 192.168.1.0/24 | sort ts'
  check tenzir 'export | where id.orig_h in fe80:5074:1b53:7e7::/64 | sort ts'
  check tenzir 'export | where id.orig_h ni fe80:5074:1b53:7e7::/64 | sort ts'
}

# bats test_tags=server,expression
@test "Expressions" {
  import_zeek_conn

  check tenzir 'export | where fe80::5074:1b53:7e7:ad4d || 169.254.225.22 | sort ts'
  check tenzir 'export | where "OrfTtuI5G4e" || fe80::5074:1b53:7e7:ad4d | sort ts'
}

# bats test_tags=server,type,ch5404
@test "Type Query" {
  import_zeek_conn "head 20"
  tenzir "export | where #schema == \"zeek.conn\""
}

# bats test_tags=server,client,import,export,transforms
@test "Export pipeline operator parsing everything but summarize" {
  import_suricata_eve

  check tenzir "export
      | sort timestamp"
  check tenzir "export
      /* a comment here */
      | select /* and a comment there /**/ timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      /**/ /*foo*/"
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp"
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp
      | hash --salt=\"abcdefghij12\" flow_id"
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp
      | hash --salt=\"abcdefghij12\" flow_id
      | drop flow_id"
}

# bats test_tags=server,client,import,export,transforms
@test "Export pipeline operator parsing after expression" {
  import_suricata_eve

  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id'
}

# bats test_tags=import,export
@test "Patterns" {
  import_suricata_eve

  check tenzir 'export | where event_type == /.*flow$/  | sort timestamp'
  check tenzir 'export | where event_type == /.*FLOW$/i | sort timestamp'
}

# bats test_tags=pipelines,comments
@test "Comments" {
  import_suricata_eve

  check tenzir 'export | sort timestamp | select timestamp /*double beginning /* is valid */'
  check ! tenzir 'export | sort timestamp | select timestamp | /**/'
  check ! tenzir 'export | sort timestamp | select timestamp /*double ending*/ slash*/'
}

# bats test_tags=server,import,export,cef
@test "CEF" {
  tenzir "from ${INPUTSDIR}/cef/cynet.log read cef | import"
  tenzir "from ${INPUTSDIR}/cef/checkpoint.log read cef | import"
  tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | import"

  tenzir 'export | where cef_version >= 0 && device_vendor == "Cynet"'
  tenzir 'export | where 172.31.5.93'
  tenzir 'export | where act == /Accept|Bypass/'
  tenzir 'export | where dvc == 10.1.1.8'
}

# bats test_tags=import, export, pipelines
@test "Sort with Remote Operators" {
  check tenzir "from ${INPUTSDIR}/cef/forcepoint.log read cef | import"
  check tenzir 'export | sort signature_id asc | write json'
  check tenzir 'export | sort uid asc | head 10 | write json'
}

# bats test_tags=pipelines,yaml
@test "YAML" {
  TENZIR_EXAMPLE_YAML="$(dirname "$BATS_TEST_DIRNAME")/../../tenzir.yaml.example"

  check tenzir "from file ${TENZIR_EXAMPLE_YAML} read yaml | put plugins=tenzir.plugins, commands=tenzir.start.commands"
  TENZIR_LEGACY=false check tenzir 'from config() | drop tenzir.cacert?, tenzir["cache-directory"]?, tenzir["runtime-prefix"]?, tenzir["state-directory"]?, tenzir.endpoint | write_yaml'
  check tenzir "from file ${INPUTSDIR}/zeek/zeek.json read zeek-json | head 5 | write yaml"
  check tenzir 'plugins | where name == "yaml" | repeat 10 | write yaml | read yaml'
}

# bats test_tags=pipelines,flaky
@test "Blob Type" {
  # TODO: Figure out why this is flaky and re-enable the test.
  skip "Disabled due to CI flakiness"

  check tenzir "from ${INPUTSDIR}/suricata/eve.json read suricata | where :blob != null | import"

  check tenzir 'export | where :blob != null | sort timestamp'
  check tenzir 'export | sort timestamp | write csv'
  check tenzir 'export | sort timestamp | write json'
  check tenzir 'export | sort timestamp | write yaml'
  check tenzir 'export | sort timestamp | write tsv'
}
