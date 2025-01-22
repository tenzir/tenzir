: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
  export TENZIR_EXEC__DUMP_PIPELINE=true
  export TENZIR_TQL2=true
}

@test "unknown udo" {
  check -c "tenzir 'one' | head -n 1"
}

@test "invalid udo" {
  export TENZIR_OPERATORS__one="from {"
  check ! tenzir 'from {}'
}

@test "valid udo" {
  export TENZIR_OPERATORS__two="let \$two = 2 | from {} | repeat \$two"
  check tenzir 'two'
}

@test "shadowing udo" {
  export TENZIR_OPERATORS__api="from {}"
  check tenzir 'api'
}
