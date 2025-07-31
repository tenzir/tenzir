//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/ip.hpp"

#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/test/test.hpp"

#include <unordered_map>

using namespace tenzir;
using namespace std::string_literals;

namespace {

std::array<tenzir::ip::byte_type, 32> seed_1
  = {21,  34,  23,  141, 51,  164, 207, 128, 19, 10, 91, 22, 73, 144, 125, 16,
     216, 152, 143, 131, 121, 121, 101, 39,  98, 87, 76, 45, 42, 132, 34,  2};

std::array<tenzir::ip::byte_type, 32> seed_2
  = {0x80, 0x09, 0xAB, 0x3A, 0x60, 0x54, 0x35, 0xBE, 0xA0, 0xC3, 0x85,
     0xBE, 0xA1, 0x84, 0x85, 0xD8, 0xB0, 0xA1, 0x10, 0x3D, 0x65, 0x90,
     0xBD, 0xF4, 0x8C, 0x96, 0x8B, 0xE5, 0xDE, 0x53, 0x83, 0x6E};

std::array<tenzir::ip::byte_type, 32> seed_3
  = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
     16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};

void check_address_pseudonymization(
  const std::unordered_map<std::string, std::string>& addresses,
  const std::array<tenzir::ip::byte_type, 32>& seed) {
  for (const auto& [original, pseudonymized] : addresses) {
    auto original_address = *to<ip>(original);
    auto pseudonymized_address_expectation = *to<ip>(pseudonymized);
    auto pseudonymized_adress_actual = ip::pseudonymize(original_address, seed);
    REQUIRE_EQUAL(pseudonymized_adress_actual,
                  pseudonymized_address_expectation);
  }
}

} // namespace

TEST("IPv4") {
  ip x;
  ip y;
  CHECK(x == y);
  CHECK(! x.is_v4());
  CHECK(x.is_v6());

  auto a = *to<ip>("172.16.7.1");
  CHECK(to_string(a) == "172.16.7.1");
  CHECK(a.is_v4());
  CHECK(! a.is_v6());
  CHECK(! a.is_loopback());
  CHECK(! a.is_multicast());
  CHECK(! a.is_broadcast());

  auto localhost = *to<ip>("127.0.0.1");
  CHECK(to_string(localhost) == "127.0.0.1");
  CHECK(localhost.is_v4());
  CHECK(localhost.is_loopback());
  CHECK(! localhost.is_multicast());
  CHECK(! localhost.is_broadcast());

  // Lexicalgraphical comparison.
  CHECK(localhost < a);

  // Bitwise operations
  ip anded = a & localhost;
  ip ored = a | localhost;
  ip xored = a ^ localhost;
  CHECK(anded == *to<ip>("44.0.0.1"));
  CHECK(ored == *to<ip>("255.16.7.1"));
  CHECK(xored == *to<ip>("211.16.7.0"));
  CHECK(anded.is_v4());
  CHECK(ored.is_v4());
  CHECK(xored.is_v4());

  auto broadcast = *to<ip>("255.255.255.255");
  CHECK(broadcast.is_broadcast());

  uint32_t n = 3232235691;
  auto b = ip::v4(n);
  CHECK(to_string(b) == "192.168.0.171");

  auto n8n = std::array<uint8_t, 4>{{0xC0, 0xA8, 0x00, 0xAB}};
  auto b8n = ip::v4(std::span{n8n});
  CHECK(to_string(b8n) == "192.168.0.171");
}

TEST("IPv6") {
  CHECK(ip() == *to<ip>("::"));

  auto a = *to<ip>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  auto b = *to<ip>("2001:db8:0:0:202:b3ff:fe1e:8329");
  auto c = *to<ip>("2001:db8::202:b3ff:fe1e:8329");
  CHECK(a.is_v6() && b.is_v6() && c.is_v6());
  CHECK(! (a.is_v4() || b.is_v4() || c.is_v4()));
  CHECK(a == b && b == c);

  auto d = *to<ip>("ff01::1");
  CHECK(d.is_multicast());

  CHECK((a ^ b) == *to<ip>("::"));
  CHECK((a & b) == a);
  CHECK((a | b) == a);
  CHECK((a & d) == *to<ip>("2001::1"));
  CHECK((a | d) == *to<ip>("ff01:db8::202:b3ff:fe1e:8329"));
  CHECK((a ^ d) == *to<ip>("df00:db8::202:b3ff:fe1e:8328"));

  uint8_t raw8[16] = {0xdf, 0x00, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
                      0x02, 0x02, 0xb3, 0xff, 0xfe, 0x1e, 0x83, 0x28};
  auto e = ip::v6(std::span{raw8});
  CHECK(e == (a ^ d));

  uint32_t raw32[4] = {0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328};
  auto f = ip::v6(std::span{raw32});
  CHECK(f == (a ^ d));
  CHECK(f == e);

  CHECK(! a.mask(129));
  CHECK(a.mask(128)); // No modification
  CHECK(a == *to<ip>("2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
  CHECK(a.mask(112));
  CHECK(a == *to<ip>("2001:db8::202:b3ff:fe1e:0"));
  CHECK(a.mask(100));
  CHECK(a == *to<ip>("2001:db8::202:b3ff:f000:0"));
  CHECK(a.mask(64));
  CHECK(a == *to<ip>("2001:db8::"));
  CHECK(a.mask(3));
  CHECK(a == *to<ip>("2000::"));
  CHECK(a.mask(0));
  CHECK(a == *to<ip>("::"));
}

TEST("ip parseable") {
  auto p = make_parser<ip>{};
  MESSAGE("IPv4");
  auto str = "192.168.0.1"s;
  auto f = str.begin();
  auto l = str.end();
  ip a;
  CHECK(p(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v4());
  CHECK(to_string(a) == str);
  MESSAGE("IPv6");
  str = "::";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "beef::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
  str = "f00::cafe";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, a));
  CHECK(f == l);
  CHECK(a.is_v6());
  CHECK(to_string(a) == str);
}

TEST("IPv4 pseudonymization - seed 1") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"128.11.68.132", "135.242.180.132"},
    {"129.118.74.4", "134.136.186.123"},
    {"130.132.252.244", "133.68.164.234"},
    {"141.223.7.43", "141.167.8.160"},
    {"141.233.145.108", "141.129.237.235"},
    {"152.163.225.39", "151.140.114.167"},
    {"156.29.3.236", "147.225.12.42"},
    {"165.247.96.84", "162.9.99.234"},
    {"166.107.77.190", "160.132.178.185"},
    {"192.102.249.13", "252.138.62.131"},
    {"192.215.32.125", "252.43.47.189"},
    {"192.233.80.103", "252.25.108.8"},
    {"192.41.57.43", "252.222.221.184"},
    {"193.150.244.223", "253.169.52.216"},
    {"195.205.63.100", "255.186.223.5"},
    {"198.200.171.101", "249.199.68.213"},
    {"198.26.132.101", "249.36.123.202"},
    {"198.36.213.5", "249.7.21.132"},
    {"198.51.77.238", "249.18.186.254"},
    {"199.217.79.101", "248.38.184.213"},
    {"202.49.198.20", "245.206.7.234"},
    {"203.12.160.252", "244.248.163.4"},
    {"204.184.162.189", "243.192.77.90"},
    {"204.202.136.230", "243.178.4.198"},
    {"204.29.20.4", "243.33.20.123"},
    {"205.178.38.67", "242.108.198.51"},
    {"205.188.147.153", "242.96.16.101"},
    {"205.188.248.25", "242.96.88.27"},
    {"205.245.121.43", "242.21.121.163"},
    {"207.105.49.5", "241.118.205.138"},
    {"207.135.65.238", "241.202.129.222"},
    {"207.155.9.214", "241.220.250.22"},
    {"207.188.7.45", "241.255.249.220"},
    {"207.25.71.27", "241.33.119.156"},
    {"207.33.151.131", "241.1.233.131"},
    {"208.147.89.59", "227.237.98.191"},
    {"208.234.120.210", "227.154.67.17"},
    {"208.28.185.184", "227.39.94.90"},
    {"208.52.56.122", "227.8.63.165"},
    {"209.12.231.7", "226.243.167.8"},
    {"209.238.72.3", "226.6.119.243"},
    {"209.246.74.109", "226.22.124.76"},
    {"209.68.60.238", "226.184.220.233"},
    {"209.85.249.6", "226.170.70.6"},
    {"212.120.124.31", "228.135.163.231"},
    {"212.146.8.236", "228.19.4.234"},
    {"212.186.227.154", "228.59.98.98"},
    {"212.204.172.118", "228.71.195.169"},
    {"212.206.130.201", "228.69.242.193"},
    {"216.148.237.145", "235.84.194.111"},
    {"216.157.30.252", "235.89.31.26"},
    {"216.184.159.48", "235.96.225.78"},
    {"216.227.10.221", "235.28.253.36"},
    {"216.254.18.172", "235.7.16.162"},
    {"216.32.132.250", "235.192.139.38"},
    {"216.35.217.178", "235.195.157.81"},
    {"24.0.250.221", "100.15.198.226"},
    {"24.13.62.231", "100.2.192.247"},
    {"24.14.213.138", "100.1.42.141"},
    {"24.5.0.80", "100.9.15.210"},
    {"24.7.198.88", "100.10.6.25"},
    {"24.94.26.44", "100.88.228.35"},
    {"38.15.67.68", "64.3.66.187"},
    {"4.3.88.225", "124.60.155.63"},
    {"63.14.55.111", "95.9.215.7"},
    {"63.195.241.44", "95.179.238.44"},
    {"63.97.7.140", "95.97.9.123"},
    {"64.14.118.196", "0.255.183.58"},
    {"64.34.154.117", "0.221.154.117"},
    {"64.39.15.238", "0.219.7.41"},
    {"129.69.205.36", "134.182.53.212"},
    {"129.69.215.37", "134.182.41.43"},
    {"127.0.0.1", "33.0.243.129"},
    {"0.0.0.0", "120.255.240.1"},
    {"10.0.1.128", "117.15.1.129"},
    {"169.254.100.50", "169.251.68.45"},
    {"255.255.255.255", "206.120.97.255"},
  };
  check_address_pseudonymization(addresses, seed_1);
}

TEST("IPv4 pseudonymization - seed 2") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"123.123.123.123", "117.8.135.123"}, {"131.159.1.42", "162.112.255.43"},
    {"8.8.8.8", "55.21.62.136"},          {"255.8.1.100", "240.232.0.156"},
    {"0.0.0.0", "56.131.176.115"},        {"255.255.255.255", "240.15.248.0"}};
  check_address_pseudonymization(addresses, seed_2);
}

TEST("IPv4 pseudonymization - seed 3") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"192.0.2.1", "2.90.93.17"},          {"0.0.0.0", "254.152.65.220"},
    {"10.0.1.128", "246.35.190.47"},      {"127.0.0.1", "168.227.160.61"},
    {"165.254.100.50", "90.1.157.13"},    {"255.255.255.255", "56.0.15.254"},
    {"148.88.132.153", "106.38.130.153"}, {"148.88.132.64", "106.38.130.64"},
    {"148.88.133.200", "106.38.131.223"},
  };
  check_address_pseudonymization(addresses, seed_3);
}

TEST("IPv6 pseudonymization - seed 1") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"::1", "78ff:f001:9fc0:20df:8380:b1f1:704:ed"},
    {"::2", "78ff:f001:9fc0:20df:8380:b1f1:704:ef"},
    {"::ffff", "78ff:f001:9fc0:20df:8380:b1f1:704:f838"},
    {"2001:db8::1", "4401:2bc:603f:d91d:27f:ff8e:e6f1:dc1e"},
    {"2001:db8::2", "4401:2bc:603f:d91d:27f:ff8e:e6f1:dc1c"},
  };
  check_address_pseudonymization(addresses, seed_1);
}

TEST("IPv6 pseudonymization - seed 2") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"2a02:0db8:85a3:0000:0000:8a2e:0370:7344", "1482:f447:75b3:f1f9:fbdf:622e:"
                                                "34f:ff7b"},
    {"2a02:db8:85a3:0:0:8a2e:370:7344", "1482:f447:75b3:f1f9:fbdf:622e:34f:"
                                        "ff7b"},
    {"2a02:db8:85a3::8a2e:370:7344", "1482:f447:75b3:f1f9:fbdf:622e:34f:ff7b"},
    {"2a02:0db8:85a3:08d3:1319:8a2e:0370:7344", "1482:f447:75b3:f904:c1d9:ba2e:"
                                                "489:1346"},
    {"2001:b8:a3:00:00:2e:70:44", "1f18:b37b:1cc3:8118:41f:9fd1:f875:fab8"},
    {"fc00::", "f33c:8ca3:ef0f:e019:e7ff:f1e3:f91f:f800"},
  };
  check_address_pseudonymization(addresses, seed_2);
}

TEST("IPv6 pseudonymization - seed 3") {
  // test data from
  // https://github.com/noinkling/node-cryptopan/blob/main/src/test/test_data.ts
  std::unordered_map<std::string, std::string> addresses = {
    {"2001:db8::1", "dd92:2c44:3fc0:ff1e:7ff9:c7f0:8180:7e00"},
  };
  check_address_pseudonymization(addresses, seed_3);
}
