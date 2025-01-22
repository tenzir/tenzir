: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "parse a single message" {
  # This is technically not valid GELF, but we read it nonetheless.
  jq -cn '{x: 1, y: "foo", z: 4.2}' | check tenzir "read gelf"
}

@test "parse multiple messages" {
  # This test ensures that we correctly identify the NULL byte as separator.
  jq -cn '{x: 1}, {x: 2}, {x: 3}' | tr '\n' '\0' | check tenzir "read gelf"
}

@test "unflatten underscores" {
  check tenzir 'shell "jq -cn \"{x_y_z: 1}\"" | read json | unflatten _'
  check tenzir 'shell "jq -cn \"{_x_y_z: 1}\"" | read json | unflatten _'
  check tenzir 'shell "jq -cn \"{x_y_z_: 1}\"" | read json | unflatten _'
  check tenzir 'shell "jq -cn \"{_x_y_z_: 1}\"" | read json | unflatten _'
  check tenzir 'shell "jq -cn \"{x__y_z: 1}\"" | read json | unflatten _'
}
