//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE flow

#include "vast/flow.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/ip.hpp"
#include "vast/concept/parseable/vast/port.hpp"
#include "vast/ip.hpp"
#include "vast/port.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

struct fixture {
  flow tcp_flow = flow{unbox(to<ip>("10.10.0.1")), unbox(to<ip>("10.10.0.2")),
                       unbox(to<port>("123/tcp")), unbox(to<port>("321/tcp"))};

  flow udp_flow = flow{unbox(to<ip>("10.10.0.1")), unbox(to<ip>("10.10.0.2")),
                       unbox(to<port>("123/udp")), unbox(to<port>("321/udp"))};
};

} // namespace

FIXTURE_SCOPE(flow_tests, fixture)

TEST(default constructed) {
  flow x;
  flow y;
  CHECK_EQUAL(x, y);
  CHECK_EQUAL(hash(x), hash(y));
}

TEST(distinct port) {
  CHECK_EQUAL(tcp_flow.src_addr, udp_flow.src_addr);
  CHECK_EQUAL(tcp_flow.dst_addr, udp_flow.dst_addr);
  CHECK_EQUAL(tcp_flow.src_port.number(), udp_flow.src_port.number());
  CHECK_EQUAL(tcp_flow.dst_port.number(), udp_flow.dst_port.number());
  CHECK_EQUAL(protocol(tcp_flow), port_type::tcp);
  CHECK_EQUAL(protocol(udp_flow), port_type::udp);
  CHECK_NOT_EQUAL(tcp_flow, udp_flow);
  CHECK_NOT_EQUAL(hash(tcp_flow), hash(udp_flow));
}

TEST(STL hashing) {
  std::hash<flow> f;
  CHECK_EQUAL(f(tcp_flow), hash(tcp_flow));
  CHECK_EQUAL(f(udp_flow), hash(udp_flow));
}

FIXTURE_SCOPE_END()
