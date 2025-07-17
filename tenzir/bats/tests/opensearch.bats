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

search() {
  if ! command -v python; then
    skip "python executable must be in PATH"
  fi
  check --bg server python "${MISCDIR}/scripts/webserver.py" --port=$1
  wait_for_tcp "$1"
  check tenzir "from {x: 1, ip: 10.10.10.10}, {id: \"1.1.1.1\", doc: {}}, {id: \"Something\", doc: {x: 10.10.10.10}} | $2, compress=false"
  wait_all ${server[@]}
}

@test "normal case" {
  search 9202 'to_opensearch "localhost:9202", tls=false, action="index", index="main"'
}

@test "doc param" {
  search 9203 'to_opensearch "localhost:9203", tls=false, action="upsert", index="main", id=id, doc=doc.otherwise(this)'
}

@test "missing id" {
  search 9204 'to_opensearch "localhost:9204", tls=false, action="delete", id=id, index="main"'
}
