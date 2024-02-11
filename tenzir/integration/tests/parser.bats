: "${BATS_TEST_TIMEOUT:=10}"

setup() {
  export TENZIR_EXEC__DUMP_AST=true
  export TENZIR_EXEC__TQL2=true
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "empty" {
  check tenzir ""
}

@test "simple" {
  check tenzir -f /dev/stdin <<EOF
export
foo = 42
set bar = 43
foo bar, baz=qux, quux
import
EOF
}

@test "using pipes" {
  check tenzir -f /dev/stdin <<EOF
export | foo = 42
| set bar = 43 |
foo bar, baz=qux, quux | import
EOF
}

@test "arithmetic" {
  check tenzir -f /dev/stdin <<EOF
foo = 1 + 2 * 3 + 4 + 5 * 6 * 7 / 8 / 9
foo = ((1 + (2 * 3)) + 4) + ((((5 * 6) * 7) / 8) / 9)
EOF
}

@test "multiline operators" {
  # TODO
  check ! tenzir -f /dev/stdin <<EOF
a b,
  c,
  d, e
f g
h \
  i,
  j\
  ,k
l m +
  n
EOF
}

@test "control flow" {
  # TODO
  check ! tenzir -f /dev/stdin <<EOF
a
if b == 42 {
  c d
  e f=g
}
h
if i {} else {}
j
match k {}
EOF
}
