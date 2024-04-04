: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "loader - connect" {
  coproc SERVER {
    exec echo foo | socat - TCP-LISTEN:8000
  }
  # TODO: this works: socat - TCP4:127.0.0.1:8000
  # Why can't Tenzir connect?
  check tenzir "load tcp://127.0.0.1:8000 --connect"
}

@test "loader - listen once" {
  check --bg listen \
    tenzir "load tcp://127.0.0.1:3000 --listen-once"
  timeout 10 bash -c 'until lsof -i :3000; do sleep 0.2; done'
  echo foo | socat - TCP4:127.0.0.1:3000
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
    tenzir "load tcp://127.0.0.1:4000 --listen-once --tls --certfile ${key_and_cert} --keyfile ${key_and_cert}"
  timeout 10 bash -c 'until lsof -i :4000; do sleep 0.2; done'
  echo foo | openssl s_client 127.0.0.1:4000
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
    tenzir "from tcp://127.0.0.1:4000 --tls --certfile ${key_and_cert} --keyfile ${key_and_cert} read json | deduplicate foo | head 2 | sort foo"
  timeout 10 bash -c 'until lsof -i :4000; do sleep 0.2; done'
  coproc CLIENT1 {
    while true; do
      jq -n '{foo: 1}'
      sleep 1
    done | openssl s_client 127.0.0.1:4000
  }
  coproc CLIENT2 {
    while true; do
      jq -n '{foo: 2}'
      sleep 1
    done | openssl s_client 127.0.0.1:4000
  }
  wait_all "${listen[@]}"
  kill "${CLIENT1_PID}" "${CLIENT2_PID}"
  rm "${key_and_cert}"
}

@test "saver - connect" {
  coproc SERVER {
    check exec socat TCP-LISTEN:7000 -
  }
  echo foo | tenzir "save tcp://127.0.0.1:7000"
}

@test "saver - listen" {
  coproc SERVER {
    echo foo | tenzir "save tcp://127.0.0.1:6000 --listen --listen-once"
  }
  timeout 10 bash -c 'until lsof -i :6000; do sleep 0.2; done'
  check socat TCP4:127.0.0.1:6000 -
}
