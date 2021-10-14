//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/address.hpp"

#include "vast/address.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/address.hpp"

#define SUITE address
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(IPv4) {
  address x;
  address y;
  CHECK(x == y);
  CHECK(!x.is_v4());
  CHECK(x.is_v6());

  auto a = *to<address>("172.16.7.1");
  CHECK(to_string(a) == "172.16.7.1");
  CHECK(a.is_v4());
  CHECK(!a.is_v6());
  CHECK(!a.is_loopback());
  CHECK(!a.is_multicast());
  CHECK(!a.is_broadcast());

  auto localhost = *to<address>("127.0.0.1");
  CHECK(to_string(localhost) == "127.0.0.1");
  CHECK(localhost.is_v4());
  CHECK(localhost.is_loopback());
  CHECK(!localhost.is_multicast());
  CHECK(!localhost.is_broadcast());

  // Lexicalgraphical comparison.
  CHECK(localhost < a);

  // Bitwise operations
  address anded = a & localhost;
  address ored = a | localhost;
  address xored = a ^ localhost;
  CHECK(anded == *to<address>("44.0.0.1"));
  CHECK(ored == *to<address>("255.16.7.1"));
  CHECK(xored == *to<address>("211.16.7.0"));
  CHECK(anded.is_v4());
  CHECK(ored.is_v4());
  CHECK(xored.is_v4());

  auto broadcast = *to<address>("255.255.255.255");
  CHECK(broadcast.is_broadcast());

  uint32_t n = 3232235691;
  auto b = address::v4(n);
  CHECK(to_string(b) == "192.168.0.171");

  auto n8n = std::array<uint8_t, 4>{{0xC0, 0xA8, 0x00, 0xAB}};
  auto b8n = address::v4(std::span{n8n});
  CHECK(to_string(b8n) == "192.168.0.171");
}

TEST(IPv6) {
  CHECK(address() == *to<address>("::"));

  auto a = *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  auto b = *to<address>("2001:db8:0:0:202:b3ff:fe1e:8329");
  auto c = *to<address>("2001:db8::202:b3ff:fe1e:8329");
  CHECK(a.is_v6() && b.is_v6() && c.is_v6());
  CHECK(!(a.is_v4() || b.is_v4() || c.is_v4()));
  CHECK(a == b && b == c);

  auto d = *to<address>("ff01::1");
  CHECK(d.is_multicast());

  CHECK((a ^ b) == *to<address>("::"));
  CHECK((a & b) == a);
  CHECK((a | b) == a);
  CHECK((a & d) == *to<address>("2001::1"));
  CHECK((a | d) == *to<address>("ff01:db8::202:b3ff:fe1e:8329"));
  CHECK((a ^ d) == *to<address>("df00:db8::202:b3ff:fe1e:8328"));

  uint8_t raw8[16] = {0xdf, 0x00, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00,
                      0x02, 0x02, 0xb3, 0xff, 0xfe, 0x1e, 0x83, 0x28};
  auto e = address::v6(std::span{raw8});
  CHECK(e == (a ^ d));

  uint32_t raw32[4] = {0xdf000db8, 0x00000000, 0x0202b3ff, 0xfe1e8328};
  auto f = address::v6(std::span{raw32});
  CHECK(f == (a ^ d));
  CHECK(f == e);

  CHECK(!a.mask(129));
  CHECK(a.mask(128)); // No modification
  CHECK(a == *to<address>("2001:db8:0000:0000:0202:b3ff:fe1e:8329"));
  CHECK(a.mask(112));
  CHECK(a == *to<address>("2001:db8::202:b3ff:fe1e:0"));
  CHECK(a.mask(100));
  CHECK(a == *to<address>("2001:db8::202:b3ff:f000:0"));
  CHECK(a.mask(64));
  CHECK(a == *to<address>("2001:db8::"));
  CHECK(a.mask(3));
  CHECK(a == *to<address>("2000::"));
  CHECK(a.mask(0));
  CHECK(a == *to<address>("::"));
}

TEST(parseable) {
  auto p = make_parser<address>{};
  MESSAGE("IPv4");
  auto str = "192.168.0.1"s;
  auto f = str.begin();
  auto l = str.end();
  address a;
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
