: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

# bats test_tags=leef
@test "Parse LEEF" {
  check tenzir "from ${INPUTSDIR}/leef/leef1.log read leef"
  check tenzir "from ${INPUTSDIR}/leef/leef2.log read leef"
}

# bats test_tags=syslog,leef
@test "Parse LEEF over syslog" {
  check tenzir "from ${INPUTSDIR}/syslog/leef-over-syslog.log read syslog"
  check tenzir "from ${INPUTSDIR}/syslog/leef-over-syslog.log read syslog | parse content leef"
  check tenzir "from ${INPUTSDIR}/syslog/zscaler-nss.log read syslog | parse content leef"
}
