: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "parse null delimited events" {
  # This test ensures that we correctly identify the NULL byte as separator.
  printf "1 2 3" | tr ' ' '\0' | check tenzir "read lines --null"
}
