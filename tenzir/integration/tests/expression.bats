: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export TENZIR_EXEC__TQL2=true
}

@test "add" {
  check tenzir -f /dev/stdin <<EOF
source {}

x0 = 1 + 2
x1 = uint("1") + 2
x2 = 1 + uint("2")
x3 = uint("1") + uint("2")
x4 = 9223372036854775807 + 1
x5 = 18446744073709551615
x6 = x5 + 1
x7 = 1.0 + 2.0
x8 = 1.0 + 2
x9 = uint("1") + 2.0
x10 = "a" + "b"
x11 = "a" + 123
// TODO: String wrapping should not be necessary.
x12 = int("-9223372036854775808") + -1
x13 = null + 1
x14 = int(null) + 1

write_json ndjson=false
EOF
}

@test "record spread" {
  check tenzir -f '/dev/stdin' <<EOF
source {}
x = {...this, x: 1}
y = {y: 2, ...this}
z = {...x, ...42, ...y}
write_json
EOF
}

@test "list indexing" {
  check tenzir -f '/dev/stdin' <<EOF
source [
  { a: [1, 2, 3] },
  { a: [4, 5] },
]
b = a[0]
c = a[2]
d = a[-1]
e = a[-3]
EOF
}
