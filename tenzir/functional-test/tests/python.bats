: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  # Fake HTTP server accepts any input.
  conn=$(mktemp)
  data=$(mktemp)
  coproc SRV { exec socat -d -d -lf "${conn}" -r /dev/stdout TCP-LISTEN:0,crlf,reuseaddr,fork SYSTEM:"echo HTTP/1.0 200; echo Content-Type\: text/plain;" >> "${data}"; }
  sync "${conn}"
  read -r HTTP_INFO < "${conn}"
  port="${HTTP_INFO##*:}"
}

teardown() {
  kill -9 "$SRV_PID"
  rm "${conn}" "${data}"
}

@test "python operator simple" {
  check tenzir -f /dev/stdin << END
    show version
    | put a=1, b=2
    | python "c = a + b"
END
}

@test "python operator build info" {
  check tenzir -f /dev/stdin << END
    show build
    | python "sanitizers.address = 3; del sanitizers.undefined_behavior; del type"
END
}

@test "python operator HTTP requests" {
  check tenzir -f /dev/stdin << END
    show version
    | put a=1, b=2
    | python '
import requests
_payload = {"a": a, "b": b}
requests.post("http://127.0.0.1:$port", data=_payload)'
    | discard
END
  check grep -v '\(Host:\|User-Agent:\)' "${data}"
  truncate -s 0 "${data}"
}

# TODO: Missing tests
# * more types from the data model: int, ip, subnet, enum, list, list of record
# * produce those types as output
