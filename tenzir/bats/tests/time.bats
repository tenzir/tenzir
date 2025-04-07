: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

@test "Duration Arithmetic" {
  check tenzir 'from {x: -3.5m + (-30s) + (1h / 2.5) - 20m * (10d/20d)}'
}

@test "Rounding" {
  check tenzir 'from { time: 2024-08-27T16:42:43 } | year = round(time, 1y) | half = round(time, 0.5y) | week = round(time, 1w) | nano = round(time, 1ns)'
  check tenzir 'from { duration: 120d } | year = round(duration, 1y) | half = round(duration, 0.5y) | week = round(duration, 1w) | nano = round(duration, 1ns)'
  check tenzir 'from { duration: -120d } | year = round(duration, 1y) | half = round(duration, 0.5y) | week = round(duration, 1w) | nano = round(duration, 1ns)'
  check tenzir 'from {} | a = ceil(2s, 2s) | b = ceil(1s, 2s) | c = ceil(3s, 2s) | d = ceil(5s, 3s)'
  check tenzir 'from {} | a = ceil(-2s, 2s) | b = ceil(-1s, 2s) | c = ceil(-3s, 2s) | d = ceil(-5s, 3s)'
  check tenzir 'from {} | a = floor(2s, 2s) | b = floor(1s, 2s) | c = floor(3s, 2s) | d = floor(5s, 3s)'
  check tenzir 'from {} | a = floor(-2s, 2s) | b = floor(-1s, 2s) | c = floor(-3s, 2s) | d = floor(-5s, 3s)'
  check tenzir 'from {} | a = round(2s, 2s) | b = round(1s, 2s) | c = round(4s, 3s) | d = round(5s, 3s)'
  check tenzir 'from {} | a = round(-2s, 2s) | b = round(-1s, 2s) | c = round(-4s, 3s) | d = round(-5s, 3s)'
}
