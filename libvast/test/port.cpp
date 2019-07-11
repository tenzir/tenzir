/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/concept/parseable/from_string.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/port.hpp"
#include "vast/subnet.hpp"

#define SUITE port
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(ports) {
  port p;
  CHECK(p.number() == 0u);
  CHECK(p.type() == port::unknown);
  MESSAGE("tcp");
  p = port(22u, port::tcp);
  CHECK(p.number() == 22u);
  CHECK(p.type() == port::tcp);
  MESSAGE("udp");
  port q(53u, port::udp);
  CHECK(q.number() == 53u);
  CHECK(q.type() == port::udp);
  MESSAGE("operators");
  CHECK(p != q);
  CHECK(p < q);
}

TEST(printable) {
  CHECK_EQUAL(to_string(port{42, port::unknown}), "42/?");
  CHECK_EQUAL(to_string(port{53, port::udp}), "53/udp");
  CHECK_EQUAL(to_string(port{80, port::tcp}), "80/tcp");
  CHECK_EQUAL(to_string(port{7, port::icmp}), "7/icmp");
  CHECK_EQUAL(to_string(port{7, port::icmp6}), "7/icmp6");
}

TEST(parseable) {
  port x;
  CHECK(parsers::port("42/?"s, x));
  CHECK(x == port{42, port::unknown});
  CHECK(parsers::port("7/icmp"s, x));
  CHECK(x == port{7, port::icmp});
  CHECK(parsers::port("22/tcp"s, x));
  CHECK(x == port{22, port::tcp});
  CHECK(parsers::port("53/udp"s, x));
  CHECK(x == port{53, port::udp});
  CHECK(parsers::port("7/icmp6"s, x));
  CHECK(x == port{7, port::icmp6});
  CHECK(parsers::port("80/sctp"s, x));
  CHECK(x == port{80, port::sctp});
}
