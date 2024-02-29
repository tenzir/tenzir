: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  export TENZIR_EXEC__DUMP_TOKENS=true
  export TENZIR_EXEC__TQL2=true
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "null byte" {
  check ! tenzir -f /dev/stdin < <(echo -ne '\0')
}

@test "invalid utf-8" {
  check ! tenzir -f /dev/stdin < <(echo -ne '"a\xFFb" + 42')
}

@test "valid utf-8" {
  check tenzir -f /dev/stdin < <(echo -ne '"a\xF0\x9F\xA4\xA8b"')
}

@test "non-terminated string" {
  check ! tenzir '"bar'
}

@test "non-terminated line comment" {
  check tenzir '//'
}

@test "non-terminated delim comment" {
  check ! tenzir '/*'
}

@test "tokens" {
  SOURCE=$(
    cat <<-END
foo42_bar
42
-0
31.0
-3.5
123_456_789.00
cafe::cafe
1234::5678
bad:c0ff:ee::
c0f:fee::
::f00d:cafe:
::
::1
f::
127.0.0.1
::1.2.3.4
foo::bar.2.3.4.5
0.0.0.0/42
/* hi */
/* // */
// test /*
+-*/
"foo"
"foo\"bar"
"\{foo}"
"\xFF"
f""
foo""
42kb
4h
42foo
foo'bar
foo'\$bar
2023-02-20
2023-02-20T12:34:56
2023-02-20T12:34:56+02:00
2023-02-20T12:34:56-04:00
42_042ms
42.0gb
END
  )
  check tenzir "$SOURCE"
}
