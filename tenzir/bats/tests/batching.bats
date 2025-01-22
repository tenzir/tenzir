# shellcheck disable=SC2016

: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

teardown() {
  :
}

@test "unordered batch" {
  check tenzir "from $INPUTSDIR/pcap/suricata/eve.json.gz read suricata --no-infer | repeat 10 | batch | measure | drop timestamp"
  check tenzir "from $INPUTSDIR/pcap/suricata/eve.json.gz read suricata --no-infer | repeat 10 | unordered batch | measure | drop timestamp"

  check tenzir "from $INPUTSDIR/pcap/suricata/eve.json.gz read suricata --no-infer | repeat 10 | batch 200 | measure | drop timestamp"
  check tenzir "from $INPUTSDIR/pcap/suricata/eve.json.gz read suricata --no-infer | repeat 10 | unordered batch 200 | measure | drop timestamp"
}
