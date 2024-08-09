: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export TENZIR_EXEC__TQL2=true
}

@test "add" {
  check tenzir -f /dev/stdin <<EOF
from [{}]

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
from [{}]
x = {...this, x: 1}
y = {y: 2, ...this}
z = {...x, ...42, ...y}
write_json
EOF
}

@test "list indexing" {
  check tenzir -f '/dev/stdin' <<EOF
from [
  { a: [1, 2, 3] },
  { a: [4, 5] },
]
b = a[0]
c = a[2]
d = a[-1]
e = a[-3]
EOF
}

@test "length method" {
  check tenzir -f '/dev/stdin' <<EOF
from [
  { a: null, b: null },
  { a: "", b: [] },
  { a: "foo", b: [1, 2, 3] },
]
x = a.length()
y = b.length()
EOF
}

@test "list construction" {
  check tenzir -f '/dev/stdin' <<EOF
from [{
  x0: [],
  x1: [null],
  x2: [null, null],
  x3: [null, 42],
  x4: [42, null],
  x5: [{}, {}],
  x6: [{a: 42}, {a: 42}],
  x7: [{a: 42}, {b: 42}],
  x8: [{a: null}, {b: 42, a: 42}],
  x9: [[], []],
  x10: [[], [42]],
  x11: [[{x: 42}], [{y: 42}]],
  x12: [{}, []],
  x13: [123, "abc"],
  x14: [[{x: 123}], [{x: "abc"}]],
}]
write_json
EOF
}

@test "numeric si prefixes" {
  check tenzir -f '/dev/stdin' <<EOF
from {
  x0: 1Mi,
  x1: -1M,
  x2: 1.2k,
  x3: -1.234k,
  x4: 1.2345k,
  x5: -1.0000000000003k,
  x6: 17.5E,
  x7: 18.5E,
}
write_json
EOF
}
