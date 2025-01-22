: "${BATS_TEST_TIMEOUT:=60}"

# BATS ports of our old integration test suite.

# This file contains the subset of tests that were using
# the old ServerFixture helper class.

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  setup_node_with_default_config
}

teardown() {
  teardown_node
}

# -- Tests ----------------------------------------

# bats test_tags=server,import,export,zeek
@test "Server Zeek multiple imports" {
  import_zeek_conn
  import_zeek_dns

  check tenzir 'export | where resp_h == 192.168.1.104 | extend schema=#schema | sort schema | sort --stable ts'
  check tenzir 'export | where zeek.conn.id.resp_h == 192.168.1.104 | extend schema=#schema | sort schema | sort --stable ts'
  check tenzir 'export | where :time >= 1970-01-01 && #schema != "tenzir.metrics" | summarize count=count(.)'
  check tenzir 'export | where #schema == "zeek.conn" | summarize count=count(.)'
}

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

# bats test_tags=concepts,models
@test "Taxonomy queries" {
  import_data "from ${INPUTSDIR}/pcap/zeek/conn.log.gz read zeek-tsv"
  import_data "from ${INPUTSDIR}/pcap/suricata/eve.json.gz"
  check tenzir 'export | where "net.src.ip == 192.168.168.100" | summarize count=count(.)'
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
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp
      | hash --salt=\"abcdefghij12\" flow_id
      | drop flow_id
      | pseudonymize -m \"crypto-pan\" -s \"123456abcdef\" src_ip, dest_ip"
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp
      | hash --salt=\"abcdefghij12\" flow_id
      | drop flow_id
      | pseudonymize -m \"crypto-pan\" -s \"123456abcdef\" src_ip, dest_ip
      | rename source_ip=src_ip"
  check tenzir "export
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | sort timestamp
      | drop timestamp
      | hash --salt=\"abcdefghij12\" flow_id
      | drop flow_id
      | pseudonymize -m \"crypto-pan\" -s \"123456abcdef\" src_ip, dest_ip
      | rename source_ip=src_ip
      | where #schema
==\"suricata.alert\" || #schema == \"suricata.fileinfo\""
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
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id
      | pseudonymize -m "crypto-pan" -s "123456abcdef" src_ip, dest_ip'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id
      | pseudonymize -m "crypto-pan" -s "123456abcdef" src_ip, dest_ip'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id
      | pseudonymize -m "crypto-pan" -s "123456abcdef" src_ip, dest_ip
      | rename source_ip=src_ip'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id
      | pseudonymize -m "crypto-pan" -s "123456abcdef" src_ip, dest_ip
      | rename source_ip=src_ip'
  check tenzir 'export
      | where src_ip==147.32.84.165 && (src_port==1181 || src_port == 138)
      | sort timestamp
      | pass
      | select timestamp, flow_id, src_ip, dest_ip, src_port
      | drop timestamp
      | hash --salt="abcdefghij12" flow_id
      | drop flow_id
      | pseudonymize -m "crypto-pan" -s "123456abcdef" src_ip, dest_ip
      | rename source_ip=src_ip
      | where #schema =="suricata.alert" || #schema == "suricata.fileinfo"'
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

# bats test_tags=pipelines, zeek
@test "Top and Rare Operators" {
  check tenzir "from ${INPUTSDIR}/zeek/conn.log.gz read zeek-tsv | import"
  check tenzir 'export | top id.orig_h | to stdout'
  check tenzir 'export | rare id.orig_h | to stdout'
  check tenzir 'export | top id.orig_h --count-field=amount | to stdout'
  check tenzir 'export | rare id.orig_h -c amount | to stdout'
  check ! tenzir 'export | top count | to stdout'
  check ! tenzir 'export | top id.orig_h --count-field=id.orig_h | to stdout'
  check ! tenzir 'export | rare id.orig_h -c id.orig_h | to stdout'
  check ! tenzir 'export | rare | to stdout'
  check ! tenzir 'export | top | to stdout'
  check ! tenzir 'export | top "" | to stdout'
}

# bats test_tags=pipelines,yaml
@test "YAML" {
  TENZIR_EXAMPLE_YAML="$(dirname "$BATS_TEST_DIRNAME")/../../tenzir.yaml.example"

  check tenzir "from file ${TENZIR_EXAMPLE_YAML} read yaml | put plugins=tenzir.plugins, commands=tenzir.start.commands"
  check tenzir 'config | drop tenzir.config, tenzir.cache-directory, tenzir.metrics, tenzir.state-directory, tenzir.start, tenzir.endpoint | write yaml'
  check tenzir "from file ${INPUTSDIR}/zeek/zeek.json read zeek-json | head 5 | write yaml"
  check tenzir 'plugins | where name == "yaml" | repeat 10 | write yaml | read yaml'
}

# bats test_tags=pipelines,zeek
@test "Zeek TSV with Remote Import" {
  check tenzir "from ${INPUTSDIR}/zeek/merge.log read zeek-tsv | import"
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
  check tenzir 'export | sort timestamp | write zeek-tsv --disable-timestamp-tags'
}

# bats test_tags=import,export,pipelines
@test "Export in Pipeline" {
  check --sort tenzir 'export'
  check tenzir "from file ${INPUTSDIR}/cef/cynet.log read cef | import"
  check --sort tenzir 'export'
  check --sort tenzir 'export | to stdout'
  check tenzir "from file ${INPUTSDIR}/cef/checkpoint.log read cef | import"
  check tenzir 'export | summarize length=count(.)'
  check tenzir 'export | where device_product == "VPN-1 & FireWall-1" | summarize length=count(.)'
  check tenzir 'export | where device_product == "VPN-1 & FireWall-1" | where 192.168.101.100'
  check tenzir 'export | where device_product == "VPN-1 & FireWall-1" && 192.168.101.100'
}
