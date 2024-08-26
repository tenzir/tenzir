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
