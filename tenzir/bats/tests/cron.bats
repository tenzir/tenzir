: "${BATS_TEST_TIMEOUT:=60}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  setup_node_with_default_config

  # We don't care about the actual events, just the number of them.
  export TENZIR_EXEC__IMPLICIT_EVENTS_SINK='enumerate index | select index | write_tql compact=true | save_stdout'
}

teardown() {
  teardown_node
}

@test "cron modifier" {
  check tenzir 'cron "* * * * * *", { version } | head'
  check tenzir 'cron "* * * * * *", { version } | cron "*/5 * * * * *", { tail 1 } | head 1'
}

@test "cron with remote" {
  check tenzir 'cron "* * * * * *", { remote { version  } } | head'
  check tenzir 'remote { cron "* * * * * *", { version } | head }'
}

@test "cron errors" {
  check ! tenzir 'cron "* * * * * *", { from "./this-file-does-not-exist.json" }'
  check ! tenzir 'cron "120 * * * * *", { version }'
  check ! tenzir 'cron "E * * * * *", { version }'
}
