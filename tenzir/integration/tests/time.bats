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

@test "Round" {
  check tenzir --tql2 'from { time: 2024-08-27T16:42:43 } | year = round(time, 1y) | half = round(time, 0.5y) | week = round(time, 1w) | nano = round(time, 1ns)'
  check tenzir --tql2 'from { duration: 120d } | year = round(duration, 1y) | half = round(duration, 0.5y) | week = round(duration, 1w) | nano = round(duration, 1ns)'
}
