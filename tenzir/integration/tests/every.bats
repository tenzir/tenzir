: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  setup_node_with_default_config

  # We don't care about the actual events, just the number of them.
  export TENZIR_EXEC__IMPLICIT_EVENTS_SINK="enumerate index | put index | write json --compact-output | save -"
}

teardown() {
  teardown_node
}

@test "every modifier" {
  check tenzir "every 5ms version | head"
  check tenzir "every 5ms version | every 500ms tail 1 | head 1"
}

@test "every with remote" {
  check tenzir "every 5ms remote version | head"
  check tenzir "remote every 5ms version | head"
}

@test "every errors" {
  check ! tenzir "every 1s from ./this-file-does-not-exist.json"
  check ! tenzir "every -1s version"
  check ! tenzir "every 0s version"
  check ! tenzir "every 0 version"
  check ! tenzir "every version"
}

@test "every with multiple operators" {
  export TENZIR_EXEC__TQL2=true
  check tenzir -f /dev/stdin <<EOF
every 10ms {
  from {x: 42}
  write_json
  read_json
}
head 5
EOF
  check ! tenzir -f /dev/stdin <<EOF
every 10ms {
  load_file ""
  import
}
EOF
}
