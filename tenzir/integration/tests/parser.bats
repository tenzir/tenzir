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

@test "precedence" {
  check tenzir -f /dev/stdin <<EOF
foo = 1 + 2 * 3 + 4 + 5 * 6 * 7 / 8 / 9 or -foo + not 2 and bar == 42
EOF
}

@test "multiline operators" {
  check tenzir -f /dev/stdin <<EOF
a b,
  c,
  d, e
f g
h \\
  i,
  j\\
  ,k
l m +
  n
EOF
}

@test "control flow" {
  check tenzir -f /dev/stdin <<EOF
a
if b == 42 {
  c d
  e f=g
}
h
if i {} else {}
j
match k {}
match k { "foo" => { bar } }
match k {
  "foo" => {
    bar
  }
  42 => {
    foo bar
    qux bar
  }
}
EOF
}

@test "functions" {
  check tenzir -f /dev/stdin <<EOF
a = b()
a = b(c)
a = b(c=d)
a = b(c, d=e)
a = b(c=d, e, g=h, i,)
foo a.b()
foo a.b(c)
foo a.b(c=d)
foo a.b(c, d=e)
foo a.b(c=d, e, g=h, i,)
EOF
  check ! tenzir -f /dev/stdin <<EOF
a = b(,)
EOF
  check ! tenzir -f /dev/stdin <<EOF
a = b(c,,)
EOF
}

@test "ocsf example" {
  check tenzir -f /dev/stdin <<EOF
activity_name = "Launch"
activity_id = 1
actor.process = {
  file: {
    path: path,
    parent_folder: std'path'parent(src.event_data.ParentImage),
    name: std'path'file_name(src.event_data.ParentImage),
    "type": "Unknown",
    type_id: 0,
  },
  pid: int(src.event_data.ParentProcessId),
}
drop src.event_data.ParentImage, src.event_data.ParentProcessId
actor.user = {
  account_type: "Windows Account",
  account_type_id: 2,
  domain: src.user.domain,
  name: src.user.name,
  uid: src.user.identifier,
}
drop src.user.domain, src.user.name, src.user.identifier
category_name = "System Activity"
category_uid = ocsf'category_uid(category_name)
class_name = "Process Activity"
class_uid = ocsf'class_uid(class_name)
device = {
  hostname: src.computer_name,
  os: {
    name: "Windows",
    "type": "Windows",
    type_id: 100,
  },
  "type": "Unknown",
  type_id: 0,
}
drop src.computer_name
message = "A new process has been created."
metadata = {
  original_time: src.event_data.UtcTime,
  product: {
    feature: {
      name: "Security",
    },
    name: "Microsoft Windows",
    vendor_name: "Microsoft",
  },
  profiles: ["host"],
  uid: src.record_id,
  version: "1.1.0",
}
drop src.event_data.UtcTime, src.record_id
process = {
  cmd_line: src.event_data.CommandLine,
  file: {
    path: src.event_data.Image,
    parent_folder: std'path'parent(src.event_data.Image),
    name: std'path'file_name(src.event_data.Image),
    "type": "Unknown",
    type_id: 0,
  },
  pid: int(src.event_data.ProcessId),
}
drop src.event_data.CommandLine, src.event_data.Image, src.event_data.ProcessId
severity = "Informational"
severity_id = 1
status = "Success"
status_id = 1
time = round(time(metadata.original_time).timestamp() * 1000)
type_name = "Process Activity: Launch"
type_uid = 100701
unmapped = src
drop src
EOF
}

@test "subnets" {
  check tenzir -f /dev/stdin <<EOF
from {
  x: 1.2.3.4/24,
  z: ::ffff:1.2.3.4/120,
  y: ::/1,
}
EOF
}
