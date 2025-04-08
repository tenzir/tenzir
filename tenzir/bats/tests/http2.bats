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
  load 50380 'load_http "http://localhost:50380", data={foo: "42", bar: {baz: 1, baz2: "baz2"}}'
}

@test "load HTTP POST form" {
  load 50381 'load_http "http://localhost:50381", form=true, data={foo: "42", bar: {baz: "baz"}}'
}

@test "save HTTP PUT" {
  save 51380 'version | select foo="bar" | write_json | save_http "http://localhost:51380", method="PUT"'
}

@test "save HTTP POST" {
  # save 51381 'version | select foo="bar" | to http://localhost:51381'
  save 51382 'version | select foo="bar" | write_json | save_http "http://localhost:51382", method="POST"'
}

@test "save HTTP POST delete header" {
  save 51383 'version | select foo="bar" | write_json | save_http "http://localhost:51383", headers={"Content-Type": ""}'
}

@test "save HTTP POST overwrite header" {
  save 51384 'version | select foo="bar" | write_json | save_http "http://localhost:51384", headers={"User-Agent": "Test"}'
}

@test "save HTTP POST add header" {
  save 51385 'version | select foo="bar" | write_json | save_http "http://localhost:51385", headers={"X-Test": "foo"}'
}
