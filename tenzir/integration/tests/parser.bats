: "${BATS_TEST_TIMEOUT:=3}"

setup() {
  export TENZIR_EXEC__DUMP_AST=true
  export TENZIR_EXEC__TQL2=true
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "yoyo" {
  check tenzir "std::test foo::bar"
  check tenzir "foo = { bar: baz }.bar"
}
