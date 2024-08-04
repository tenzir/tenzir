: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

@test "Parse LEEF" {
  check tenzir 'version | put timestamp="2024-07-09T16:42:43+0200" | parse timestamp time "%FT%T%z"'
}

@test "Duration Arithmetic" {
  check tenzir --tql2 'from {x: -3.5m + (-30s) + (1h / 2.5) - 20m * (10d/20d)}'
}
