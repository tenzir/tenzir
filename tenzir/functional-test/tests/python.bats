: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "python operator simple" {
  check tenzir -f /proc/self/fd/0 << END
    show version
    | put a=1, b=2
    | python "c = a + b"
END
}
