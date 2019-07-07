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

flow_tuple make_flow(protocol proto, std::string_view orig_h, uint16_t orig_p,
                     std::string_view resp_h, uint16_t resp_p) {
  auto make_addr = [](auto x) { return unbox(to<address>(x)); };
  auto make_port = [](protocol x, auto number) {
    switch (x) {
      default:
        return port{number, port::unknown};
      case protocol::icmp:
        return port{number, port::icmp};
      case protocol::tcp:
        return port{number, port::tcp};
      case protocol::udp:
        return port{number, port::tcp};
      case protocol::icmp6:
        return port{number, port::icmp};
      case protocol::sctp:
        return port{number, port::unknown}; // TODO
    }
  };
  return {proto, make_addr(orig_h), make_port(proto, orig_p), make_addr(resp_h),
          make_port(proto, resp_p)};
}

} // namespace

// Ground truth established with Christian Kreibich's Python module, e.g.:
//
//     from communityid import *
//     commid = CommunityID(seed=0, use_base64=False)
//     commid.calc(flow)
//     flow = FlowTuple(PROTO_UDP, "192.168.1.102", "192.168.1.1", 68, 67)

TEST(UDP IPv4) {
  auto x = make_flow(protocol::udp, "192.168.1.102", 68, "192.168.1.1", 67);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:69665f2c8aae6250b1286b89eb67d01a5805cc02");
  CHECK_EQUAL(b64, "1:aWZfLIquYlCxKGuJ62fQGlgFzAI=");
}

TEST(UDP IPv6) {
  auto x = make_flow(protocol::udp, "fe80::2c23:b96c:78d:e116", 58544,
                     "ff02::c", 3702);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:a6a666566d134acee99f99c74d6c83cf295ad6a3");
  CHECK_EQUAL(b64, "1:pqZmVm0TSs7pn5nHTWyDzyla1qM=");
}

TEST(TCP IPv4) {
  auto x = make_flow(protocol::tcp, "192.168.1.102", 1180, "68.216.79.113", 37);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:f4bfed67579b1f395687307fa49c92f405495b2f");
  CHECK_EQUAL(b64, "1:9L/tZ1ebHzlWhzB/pJyS9AVJWy8=");
}

TEST(TCP IPv6) {
  auto x = make_flow(protocol::tcp, "fe80::219:e3ff:fee7:5d23", 5353,
                     "ff02::fb", 53);
  auto hex = compute<policy::ascii>(x);
  auto b64 = compute<policy::base64>(x);
  CHECK_EQUAL(hex, "1:03aaaffe2842910257a2fdf52f863395cb8a4769");
  CHECK_EQUAL(b64, "1:A6qv/ihCkQJXov31L4YzlcuKR2k=");
}
