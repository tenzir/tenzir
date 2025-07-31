//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/subnet.hpp"

#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/subnet.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_literals;

TEST("subnets") {
  subnet p;
  CHECK(p.network() == *to<ip>("::"));
  CHECK(p.length() == 0);
  CHECK(to_string(p) == "::/0");

  auto a = *to<ip>("192.168.0.1");
  subnet q{a, 24 + 96};
  CHECK(q.network() == *to<ip>("192.168.0.0"));
  CHECK(q.length() == 24 + 96);
  CHECK_EQUAL(to_string(q), "192.168.0.0/24");
  CHECK(q.contains(*to<ip>("192.168.0.73")));
  CHECK(! q.contains(*to<ip>("192.168.244.73")));

  auto b = *to<ip>("2001:db8:0000:0000:0202:b3ff:fe1e:8329");
  subnet r{b, 64};
  CHECK(r.length() == 64);
  CHECK(r.network() == *to<ip>("2001:db8::"));
  CHECK(to_string(r) == "2001:db8::/64");
}

TEST("containment") {
  MESSAGE("v4");
  CHECK(to<subnet>("10.0.0.0/8")->contains(*to<ip>("10.0.0.1")));
  CHECK(to<subnet>("10.0.0.0/8")->contains(*to<subnet>("10.0.0.0/16")));
  CHECK(! to<subnet>("10.0.0.0/17")->contains(*to<subnet>("10.0.0.0/16")));
  CHECK(to<subnet>("218.89.0.0/16")->contains(*to<subnet>("218.89.167.0/24")));
  CHECK(to<subnet>("218.89.0.0/16")->contains(*to<subnet>("218.89.167.0/24")));
  CHECK(to<subnet>("218.88.0.0/14")->contains(*to<subnet>("218.89.0.0/16")));
  MESSAGE("v6");
  auto v4 = *to<subnet>("2001:db8:0000:0000:0202:b3ff:fe1e:8329/64");
  CHECK(v4.contains(*to<ip>("2001:db8::cafe:babe")));
  CHECK(! v4.contains(*to<ip>("ff00::")));
}

TEST("subnet printable") {
  auto snv4 = subnet{*to<ip>("10.0.0.0"), 8 + 96};
  CHECK_EQUAL(to_string(snv4), "10.0.0.0/8");
  auto snv6 = subnet{*to<ip>("10.0.0.0"), 8};
  CHECK_EQUAL(to_string(snv6), to_string(unbox(to<subnet>("::ffff:a00:0/8"))));
}

TEST("subnet") {
  MESSAGE("IPv4");
  auto s = unbox(to<subnet>("192.168.0.0/24"));
  CHECK_EQUAL(s, (subnet{unbox(to<ip>("192.168.0.0")), 120}));
  CHECK(s.network().is_v4());
  MESSAGE("IPv6");
  s = unbox(to<subnet>("beef::cafe/40"));
  CHECK_EQUAL(s, (subnet{*to<ip>("beef::cafe"), 40}));
  CHECK(s.network().is_v6());
}
