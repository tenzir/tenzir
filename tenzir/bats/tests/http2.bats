: "${BATS_TEST_TIMEOUT:=10}"

if [ $(uname -s) == "Darwin" ]; then
  skip "Skipping brittle HTTP tests on MacOS"
fi

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

wait_for_tcp() {
  port=$1
  timeout 10 bash -c "until lsof -i :$port; do sleep 0.2; done"
}

load() {
  if ! command -v python; then
    skip "python executable must be in PATH"
  fi
  check --bg server python "${MISCDIR}/scripts/webserver.py" --port=$1
  wait_for_tcp "$1"
  check tenzir "$2"
  wait_all ${server[@]}
}

save() {
  if ! command -v python; then
    skip "python executable must be in PATH"
  fi
  check --bg server python "${MISCDIR}/scripts/webserver.py" --port=$1
  wait_for_tcp "$1"
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
  save 50382 'version | select foo="bar" | write_json | save_http "http://localhost:50382", method="PUT"'
}

@test "save HTTP POST" {
  # save 50381 'version | select foo="bar" | to http://localhost:51381'
  save 50383 'version | select foo="bar" | write_json | save_http "http://localhost:50383", method="POST"'
}

@test "save HTTP POST delete header" {
  save 50384 'version | select foo="bar" | write_json | save_http "http://localhost:50384", headers={"Content-Type": ""}'
}

@test "save HTTP POST overwrite header" {
  save 50385 'version | select foo="bar" | write_json | save_http "http://localhost:50385", headers={"User-Agent": "Test"}'
}

@test "save HTTP POST add header" {
  save 50386 'version | select foo="bar" | write_json | save_http "http://localhost:50386", headers={"X-Test": "foo"}'
}
