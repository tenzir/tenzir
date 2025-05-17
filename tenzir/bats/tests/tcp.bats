: "${BATS_TEST_TIMEOUT:=30}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export_default_paths
  write_ssl_certs
}

wait_for_tcp() {
  port=$1
  timeout 10 bash -c "until lsof -i :$port; do sleep 0.2; done"
}

write_ssl_certs() {
  # Generates a server certificate and corresponding CA certificate, and
  # stores their filenames into ${key_and_cert} and ${cafile}.

  certdir="$(mktemp -d)"

  cat >"${certdir}/script.py" <<EOF
import trustme

ca = trustme.CA()
ca.cert_pem.write_to_path("ca.pem")

server_cert = ca.issue_cert("tenzir-node.example.org")
server_cert.private_key_and_cert_chain_pem.write_to_path("server.pem")
EOF

  if command -v uv &>/dev/null; then
    UV="uv"
  else
    UV="$(dirname "$(command -v tenzir)")/../libexec/uv"
  fi

  # It seems to be impossible to tell uv to skip its own package index with
  # uv run --with. We work around that by using the script dependencies stanza
  # mode but only if the dependency is not already available.
  python3 -m trustme --help || ${UV} add --script "${certdir}/script.py" trustme

  ${UV} run --directory "${certdir}" "${certdir}/script.py"

  key_and_cert="${certdir}/server.pem"
  cafile="${certdir}/ca.pem"
}

@test "loader - listen with SSL" {
  write_ssl_certs
  check --bg listen \
    tenzir "load_tcp \"tcp://127.0.0.1:56130\", tls=true, certfile=\"${key_and_cert}\", keyfile=\"${key_and_cert}\" { read_lines } | head 1"
  timeout 10 bash -c 'until lsof -i :56130; do sleep 0.2; done'
  echo foo | openssl s_client -CAfile ${cafile} 127.0.0.1:56130
  wait_all "${listen[@]}"
}

@test "listen with multiple connections with SSL" {
  skip "Skipping; disabled test, needs further investigation"
  check --bg listen \
    tenzir "from \"tcp://127.0.0.1:56131\", tls=true, certfile=\"${key_and_cert}\", keyfile=\"${key_and_cert}\" { read_json } | deduplicate foo | head 2 | sort foo"
  wait_for_tcp 56131
  (
    for i in {1..10}; do
      jq -n '{foo: 1}'
      sleep 1
    done | openssl s_client -CAfile ${cafile} "127.0.0.1:56131"
  ) &
  CLIENT1_PID=$!
  (
    for i in {1..10}; do
      jq -n '{foo: 2}'
      sleep 1
    done | openssl s_client -CAfile ${cafile} "127.0.0.1:56131"
  ) &
  CLIENT2_PID=$!
  wait_all "${listen[@]}" "${CLIENT1_PID}" "${CLIENT2_PID}"
}

@test "saver - connect" {
  coproc SERVER {
    check exec socat TCP-LISTEN:56132 -
  }
  echo foo | tenzir 'save_tcp "tcp://127.0.0.1:56132"'
}
