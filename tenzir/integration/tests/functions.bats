setup() {
  bats_load_library bats-support
  bats_load_library bats-assert
  bats_load_library bats-tenzir

  export TENZIR_EXEC__TQL2=true
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

@test "strftime" {
  check tenzir '
    from {
      x: 2024-12-31+12:59:42,
    }
    x = x.strftime("%Y/%m/%d - %T")
  '
}

@test "strptime" {
  check tenzir '
    from {
      x: "2024-12-31+12:59:42",
    }
    x = x.strptime("%Y-%m-%d+%H:%M:%S")
  '
}
