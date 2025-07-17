setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir
}

@test "community_id" {
  check tenzir -f /dev/stdin <<EOF
from {}
// Basic
x0 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91,
                  src_port=52482, dst_port=749, proto="tcp")
// Missing ports
x1 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91, src_port=52482, proto="tcp")
x2 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91,
                  dst_port=749, proto="tcp")
// TCP, only IPs
x3 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91, proto="tcp")
// Invalid protocol
x4 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91, proto="XXX")
// UDP
x5 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91, proto="udp")
// ICMP
x6 = community_id(src_ip=45.146.166.123, dst_ip=198.71.247.91, proto="icmp")
x7 = community_id(src_ip=192.168.0.89, dst_ip=192.168.0.1,
                  src_port=8, dst_port=0, proto="icmp")
// ICMP6
x8 = community_id(src_ip=fe80::200:86ff:fe05:80da,
                  dst_ip=fe80::260:97ff:fe07:69ea,
                  src_port=135, dst_port=0, proto="icmp6")
x9 = community_id(src_ip=3ffe:507:0:1:260:97ff:fe07:69ea,
                  dst_ip=3ffe:507:0:1:200:86ff:fe05:80da,
                  src_port=3, dst_port=0, proto="icmp6")
EOF
  check ! tenzir 'from {} | x0 = community_id( src_ip=null, dst_ip=null )'
  check ! tenzir 'from {} | x0 = community_id( src_ip=null, proto=null )'
  check ! tenzir 'from {} | x0 = community_id( dst_ip=null, proto=null )'
}

@test "has" {
  check tenzir '
    from {
      key: { field: 1 },
      error: {
        nofield: 1,
        msg: {
          field: "None"
        }
      }
    }
    key = key.has("field")
    error = error.has("field")
  '
}

@test "otherwise" {
  echo '
  {"x":1, "y":-1}
  {"x":2, "y":-2}
  {}
  {"x":4, "y":-4}
  {"x":"5", "y":-5}
  {"y":-6}
  ' |
    check tenzir 'read_json | z = x.otherwise(y)'
  check tenzir 'from {},{},{} | z = x.otherwise(-1)'
  check tenzir 'from {x:1},{x:2},{x:3} | z = x.otherwise(-1)'
  check tenzir 'from {},{x:1},{} | z = x.otherwise(-1)'
  check tenzir 'from {x:1},{},{x:2} | z = x.otherwise(-1)'
}

@test "heterogeneous otherwise" {
  check tenzir -f /dev/stdin <<EOF
from \
  {x: int(null)},
  {x: 1},
  {x: 2},
  {x: int(null)},
  {x: int(null)},
  {x: 3}
y = x.otherwise("test")
z = {x: [4 * x.otherwise(1s)], y: (x * 1s).otherwise(x * 1d)}
EOF
}

@test "select and drop matching" {
  check tenzir '
    from {
      foo: 1,
      bar: 2,
      baz: 3,
    }
    let $pattern="^ba"
    this = {
      moved: this.select_matching($pattern),
      ...this.drop_matching($pattern)
    }
  '
}

@test "replace" {
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "")
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "", max=0)
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "", max=4)
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "", max=100)
  '
  check ! tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "", max=-1)
  '
  check ! tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace(":", "", max=-100)
  '
}

@test "replace_regex" {
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f]{2}", "??")
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f]{2}", "??", max=0)
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f]{2}", "??", max=4)
  '
  check tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f]{2}", "??", max=100)
  '
  check ! tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f]{2}", "??", max=-1)
  '
  check ! tenzir '
    from {x: "85:0f:d2:e1:95:02:ab:0f:5a:c3:c8:58:f1:67:21:7d:0b:41:91:e6"}
    x = x.replace_regex("[0-9a-f{2}", "??")
  '
}

@test "slice" {
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(begin=0)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(begin=1)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(begin=-4)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=0)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=4)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=100)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=-1)
  '
  check tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=12, stride=4)
  '
  check ! tenzir '
    from {
      x: "850fd2e19502ab0f5ac3c858f167217d0b4191e6"
    }
    x = x.slice(end=6, stride=-1)
  '
}

@test "time" {
  check tenzir '
    from {}
    n = time("2024-09-24T20:20:26.168426")
    y = n.year()
    m = n.month()
    d = n.day()
  '
  # Expect failure, but only a warning
  check tenzir '
    from {
      x: 1
    }
    y = x.year()
  '
}

@test "format_time" {
  skip "This fails in CI because the timezone DB is missing"
  check tenzir '
    from {
      x: 2024-12-31+12:59:42,
    }
    x = x.format_time("%Y/%m/%d - %T")
  '
}

@test "parse_time" {
  check tenzir '
    from {
      x: "2024-12-31+12:59:42",
    }
    x = x.parse_time("%Y-%m-%d+%H:%M:%S")
  '
}

@test "parse_time with bad format string" {
  check tenzir '
    from {
      x: "2024-12-31",
    }
    x = x.parse_time("%d-%d-%d")
  '
}

@test "parse_time with bad input string" {
  check tenzir '
    from {
      x: "hello",
    }
    x = x.parse_time("%Y-%m-%d")
  '
}

@test "hex" {
  check tenzir 'from {bytes: "Tenzir"} | encoded = bytes.encode_hex()'
  check tenzir 'from {bytes: "54656E7a6972"} | decoded = bytes.decode_hex()'
}

@test "string length" {
  check tenzir -f /dev/stdin <<EOF
from {
  x: "é",
  y: "👩‍👩‍👦‍👦",
}
xa = x.length()
xb = x.length_bytes()
xc = x.length_chars()
ya = y.length()
yb = y.length_bytes()
yc = y.length_chars()
write_json
EOF
}

@test "list length" {
  check tenzir -f /dev/stdin <<EOF
from \
  { x: null },
  { x: [] },
  { x: [1, 2, 3] }
y = x.length()
EOF
}

@test "duration" {
  check tenzir 'from {x: "1.34w"}, {x: 1}, {x: ""} | x = x.duration()'
}

@test "float" {
  check tenzir 'from {x: "1.34"}, {x: " 1  "}, {x: -134}, {x: uint(null)} | x = x.float()'
}
