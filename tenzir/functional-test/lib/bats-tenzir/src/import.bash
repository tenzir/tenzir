# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

import_data() {
  local load_statement=$1
  local input_filter=${2:-}

  local pipeline="$load_statement"
  if [[ -n "$input_filter" ]]; then
    pipeline+=" | $input_filter"
  fi
  pipeline+=" | import"

  tenzir "$pipeline"
}

import_zeek_conn() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/zeek/conn.log.gz read zeek-tsv"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_zeek_dns() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/zeek/dns.log.gz read zeek-tsv"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_zeek_http() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/zeek/http.log.gz read zeek-tsv"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_zeek_snmp() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/zeek/snmp.log.gz read zeek-tsv"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_zeek_json() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/zeek/zeek.json read zeek-json"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_suricata_eve() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/suricata/eve.json"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}

import_suricata_rrdata() {
  local load_statement="from ${BATS_TENZIR_DATADIR}/inputs/suricata/rrdata-eve.json"
  local input_filter=${1:-}
  import_data "$load_statement" "$input_filter"
}
