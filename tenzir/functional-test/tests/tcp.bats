: "${BATS_TEST_TIMEOUT:=3}"

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
    tenzir "load tcp://127.0.0.1:3000 --once"
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
    tenzir "load tcp://127.0.0.1:4000 --once --tls --certfile ${key_and_cert} --keyfile ${key_and_cert}"
  timeout 10 bash -c 'until lsof -i :4000; do sleep 0.2; done'
  echo foo | openssl s_client 127.0.0.1:4000
  wait_all "${listen[@]}"
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
    echo foo | tenzir "save tcp://127.0.0.1:6000 --listen --once"
  }
  timeout 10 bash -c 'until lsof -i :6000; do sleep 0.2; done'
  check socat TCP4:127.0.0.1:6000 -
}
