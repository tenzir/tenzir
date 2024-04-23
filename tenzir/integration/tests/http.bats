: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

load() {
  if ! command -v python; then
    skip "python executable must be in PATH"
  fi
  check --bg server python "${MISCDIR}/scripts/webserver.py" --port=$1
  check tenzir "$2"
  wait_all ${server[@]}
}

save() {
  if ! command -v python; then
    skip "python executable must be in PATH"
  fi
  check --bg server python "${MISCDIR}/scripts/webserver.py" --port=$1
  tenzir "$2"
  wait_all ${server[@]}
}

@test "load HTTP POST" {
  load 50380 'load http://localhost:50380 foo=42'
}

@test "load HTTP POST form" {
  load 50381 'load http://localhost:50381 --form foo=42 bar:="baz"'
}

@test "save HTTP PUT" {
  save 51380 'version | put foo="bar" | write json | save http PUT localhost:51380'
}

@test "save HTTP POST" {
  save 51381 'version | put foo="bar" | to http://localhost:51381'
  save 51382 'version | put foo="bar" | write json | save http POST localhost:51382'
}

@test "save HTTP POST delete header" {
  save 51383 'version | put foo="bar" | to http://localhost:51383 Content-Type:'
}

@test "save HTTP POST overwrite header" {
  save 51384 'version | put foo="bar" | to http://localhost:51384 User-Agent:Test'
}

@test "save HTTP POST add header" {
  save 51385 'version | put foo="bar" | to http://localhost:51385 X-Test:foo'
}
