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

#define SUITE value_index

#include "vast/index/port_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(port) {
  port_index idx{port_type{}};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(443, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(53, port::udp))));
  REQUIRE(idx.append(make_data_view(port(8, port::icmp))));
  REQUIRE(idx.append(make_data_view(port(31337, port::unknown))));
  REQUIRE(idx.append(make_data_view(port(80, port::tcp))));
  REQUIRE(idx.append(make_data_view(port(80, port::udp))));
  REQUIRE(idx.append(make_data_view(port(80, port::unknown))));
  REQUIRE(idx.append(make_data_view(port(8080, port::tcp))));
  MESSAGE("lookup");
  port http{80, port::tcp};
  auto bm = idx.lookup(equal, make_data_view(http));
  CHECK(to_string(unbox(bm)) == "100001000");
  bm = idx.lookup(not_equal, make_data_view(http));
  CHECK_EQUAL(to_string(unbox(bm)), "011110111");
  port port80{80, port::unknown};
  bm = idx.lookup(not_equal, make_data_view(port80));
  CHECK_EQUAL(to_string(unbox(bm)), "011110001");
  port priv{1024, port::unknown};
  bm = idx.lookup(less_equal, make_data_view(priv));
  CHECK(to_string(unbox(bm)) == "111101110");
  bm = idx.lookup(greater, make_data_view(port{2, port::unknown}));
  CHECK(to_string(unbox(bm)) == "111111111");
  auto xs = list{http, port(53, port::udp)};
  auto multi = unbox(idx.lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "101001000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  port_index idx2{port_type{}};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(less_equal, make_data_view(priv));
  CHECK_EQUAL(to_string(unbox(bm)), "111101110");
}
