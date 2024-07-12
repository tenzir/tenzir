setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

# -- tests --------------------------------------------------

@test "Parse LEEF" {
  check tenzir 'version | put timestamp="2024-07-09T16:42:43+0200" | parse timestamp time "%FT%T%z"'
}
