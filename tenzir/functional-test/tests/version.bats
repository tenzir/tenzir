: "${BATS_TEST_TIMEOUT:=3}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
}

@test "print the version with the show operator" {
  run -0 --separate-stderr tenzir 'version'

  jq -e '.version' > /dev/null 2>&1 <<< "$output"
}
