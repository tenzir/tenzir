: "${BATS_TEST_TIMEOUT:=30}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

wait_for_tcp() {
  port=$1
  timeout 10 bash -c "until lsof -i :$port; do sleep 0.2; done"
}

@test "loader - connect" {
  coproc SERVER {
    exec echo foo | socat - TCP-LISTEN:56128
  }
  # TODO: this works: socat - TCP4:127.0.0.1:56128
  # Why can't Tenzir connect?
  check tenzir "load tcp://127.0.0.1:56128 --connect"
}

@test "loader - listen once" {
  check --bg listen \
    tenzir "load tcp://127.0.0.1:56129 --listen-once"
  timeout 10 bash -c 'until lsof -i :56129; do sleep 0.2; done'
  echo foo | socat - TCP4:127.0.0.1:56129
  wait_all "${listen[@]}"
}

@test "loader - listen with SSL" {
  key_and_cert=$(mktemp)
  openssl req -x509 -newkey rsa:2048 \
    -keyout "${key_and_cert}" \
    -out "${key_and_cert}" \
    -days 365 \
    -nodes \
    -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" >/dev/null 2>/dev/null
  check --bg listen \
    tenzir "load tcp://127.0.0.1:56130 --listen-once --tls --certfile ${key_and_cert} --keyfile ${key_and_cert}"
  timeout 10 bash -c 'until lsof -i :56130; do sleep 0.2; done'
  echo foo | openssl s_client 127.0.0.1:56130
  wait_all "${listen[@]}"
  rm "${key_and_cert}"
}

@test "listen with multiple connections with SSL" {
  key_and_cert=$(mktemp)
  openssl req -x509 -newkey rsa:2048 \
    -keyout "${key_and_cert}" \
    -out "${key_and_cert}" \
    -days 365 \
    -nodes \
    -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" >/dev/null 2>/dev/null
  check --bg listen \
    tenzir "from tcp://127.0.0.1:56131 --tls --certfile ${key_and_cert} --keyfile ${key_and_cert} read json | deduplicate foo | head 2 | sort foo"
  wait_for_tcp 56131
  (
    while :; do
      jq -n '{foo: 1}'
      sleep 1
    done | openssl s_client "127.0.0.1:56131"
  ) &
  CLIENT1_PID=$!
  (
    while :; do
      jq -n '{foo: 2}'
      sleep 1
    done | openssl s_client "127.0.0.1:56131"
  ) &
  CLIENT2_PID=$!
  wait_all "${listen[@]}" "${CLIENT1_PID}" "${CLIENT2_PID}"
  rm "${key_and_cert}"
}

@test "saver - connect" {
  coproc SERVER {
    check exec socat TCP-LISTEN:56132 -
  }
  echo foo | tenzir "save tcp://127.0.0.1:56132"
}

@test "saver - listen" {
  coproc SERVER {
    echo foo | tenzir "save tcp://127.0.0.1:56133 --listen --listen-once"
  }
  timeout 10 bash -c 'until lsof -i :56133; do sleep 0.2; done'
  check socat TCP4:127.0.0.1:56133 -
}
