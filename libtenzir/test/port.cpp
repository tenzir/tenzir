//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/port.hpp"

#include "tenzir/concept/printable/tenzir/port.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::string_literals;

TEST("ports") {
  port p;
  CHECK(p.number() == 0u);
  CHECK(p.type() == port_type::unknown);
  MESSAGE("tcp");
  p = port(22u, port_type::tcp);
  CHECK(p.number() == 22u);
  CHECK(p.type() == port_type::tcp);
  MESSAGE("udp");
  port q(53u, port_type::udp);
  CHECK(q.number() == 53u);
  CHECK(q.type() == port_type::udp);
  MESSAGE("operators");
  CHECK(p != q);
  CHECK(p < q);
}

TEST("port printable") {
  CHECK_EQUAL(to_string(port{42, port_type::unknown}), "42/?");
  CHECK_EQUAL(to_string(port{53, port_type::udp}), "53/udp");
  CHECK_EQUAL(to_string(port{80, port_type::tcp}), "80/tcp");
  CHECK_EQUAL(to_string(port{7, port_type::icmp}), "7/icmp");
  CHECK_EQUAL(to_string(port{7, port_type::icmp6}), "7/icmp6");
}

TEST("port parseable") {
  port x;
  CHECK(parsers::port("42/?"s, x));
  CHECK((x == port{42, port_type::unknown}));
  CHECK(parsers::port("7/icmp"s, x));
  CHECK((x == port{7, port_type::icmp}));
  CHECK(parsers::port("22/tcp"s, x));
  CHECK((x == port{22, port_type::tcp}));
  CHECK(parsers::port("53/udp"s, x));
  CHECK((x == port{53, port_type::udp}));
  CHECK(parsers::port("7/icmp6"s, x));
  CHECK((x == port{7, port_type::icmp6}));
  CHECK(parsers::port("80/sctp"s, x));
  CHECK((x == port{80, port_type::sctp}));
}
