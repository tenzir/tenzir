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

#define SUITE community_id

#include "vast/test/test.hpp"

#include "vast/community_id.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"

using namespace vast;
using namespace community_id;

namespace {

#define FLOW_FACTORY(protocol)                                                 \
  flow make_##protocol##_flow(std::string_view orig_h, uint16_t orig_p,        \
                              std::string_view resp_h, uint16_t resp_p) {      \
    constexpr auto proto = port::port_type::protocol;                          \
    return unbox(make_flow<proto>(orig_h, orig_p, resp_h, resp_p));            \
  }

FLOW_FACTORY(icmp)
FLOW_FACTORY(tcp)
FLOW_FACTORY(udp)
FLOW_FACTORY(icmp6)

#undef FLOW_FACTORY

} // namespace

// Ground truth established with Christian Kreibich's Python module, e.g.:
//
//     from communityid import *
//     commid = CommunityID(seed=0, use_base64=False)
//     commid.calc(flow)
//     flow = FlowTuple(PROTO_UDP, "192.168.1.102", "192.168.1.1", 68, 67)

TEST(UDP IPv4) {
  auto x = make_udp_flow("192.168.1.102", 68, "192.168.1.1", 67);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:69665f2c8aae6250b1286b89eb67d01a5805cc02");
  CHECK_EQUAL(b64, "1:aWZfLIquYlCxKGuJ62fQGlgFzAI=");
}

TEST(UDP IPv6) {
  auto x = make_udp_flow("fe80::2c23:b96c:78d:e116", 58544, "ff02::c", 3702);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:662f40748c18bd99d8bee39b4cf806582052611b");
  CHECK_EQUAL(b64, "1:Zi9AdIwYvZnYvuObTPgGWCBSYRs=");
}

TEST(TCP IPv4) {
  auto x = make_tcp_flow("192.168.1.102", 1180, "68.216.79.113", 37);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:f4bfed67579b1f395687307fa49c92f405495b2f");
  CHECK_EQUAL(b64, "1:9L/tZ1ebHzlWhzB/pJyS9AVJWy8=");
}

TEST(TCP IPv6) {
  auto x = make_tcp_flow("fe80::219:e3ff:fee7:5d23", 5353, "ff02::fb", 53);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:03aaaffe2842910257a2fdf52f863395cb8a4769");
  CHECK_EQUAL(b64, "1:A6qv/ihCkQJXov31L4YzlcuKR2k=");
}

TEST(ICMPv4) {
  auto x = make_icmp_flow("1.2.3.4", 0, "5.6.7.8", 8);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:d6f36bf9c570edbcd9fad1ac8761fbbe807069a6");
  CHECK_EQUAL(b64, "1:1vNr+cVw7bzZ+tGsh2H7voBwaaY=");
}

TEST(ICMPv4 oneway) {
  auto x = make_icmp_flow("192.168.0.89", 128, "192.168.0.1", 129);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:86459c1ce1ea4c65aaffe7f01c48a6e5efa0d5f1");
  CHECK_EQUAL(b64, "1:hkWcHOHqTGWq/+fwHEim5e+g1fE=");
}

TEST(ICMPv6) {
  auto x = make_icmp6_flow("fe80::200:86ff:fe05:80da", 135, "fe80::260", 136);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:ffb2d8321708804a883ac02fe6c76655499b3ff5");
  CHECK_EQUAL(b64, "1:/7LYMhcIgEqIOsAv5sdmVUmbP/U=");
}

TEST(ICMPv6 oneway) {
  auto x = make_icmp6_flow("fe80::dead", 42, "fe80::beef", 84);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:118a3bbf175529a3d55dca55c4364ec47f1c4152");
  CHECK_EQUAL(b64, "1:EYo7vxdVKaPVXcpVxDZOxH8cQVI=");
}
