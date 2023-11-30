: "${BATS_TEST_TIMEOUT:=3}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

}

@test "connecting" {
 coproc NC { exec echo foo | ncat -l 127.0.0.1 8000 ; }
 check tenzir "load tcp://127.0.0.1:8000"
}

@test "listening" {
 openssl req -x509 -newkey rsa:2048 \
   -keyout key_and_cert.pem \
   -out key_and_cert.pem \
   -days 365 \
   -nodes \
   -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=www.example.com" >/dev/null 2>/dev/null
 coproc TENZIR { tenzir "load tcp://127.0.0.1:4000 --listen --tls --certfile ./key_and_cert.pem --keyfile ./key_and_cert.pem | save file ./output.bin" || true ; }
 # TODO: Is there a way to wait until a pipeline is initialized?
 sleep 1
 echo foo | openssl s_client 127.0.0.1:4000
 wait $TENZIR_PID
 check cat output.bin
 rm key_and_cert.pem
}
