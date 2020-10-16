
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

#include "vast/index/subnet_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(subnet) {
  subnet_index idx{subnet_type{}};
  auto s0 = *to<subnet>("192.168.0.0/24");
  auto s1 = *to<subnet>("192.168.1.0/24");
  auto s2 = *to<subnet>("fe80::/10");
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s1)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s0)));
  REQUIRE(idx.append(make_data_view(s2)));
  REQUIRE(idx.append(make_data_view(s2)));
  MESSAGE("address lookup (ni)");
  auto a = unbox(to<address>("192.168.0.0")); // network address
  auto bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.0.1"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.1.42"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  // IPv6
  a = unbox(to<address>("feff::")); // too far out
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  a = unbox(to<address>("fe80::aaaa"));
  bm = idx.lookup(ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000011");
  MESSAGE("equality lookup");
  bm = idx.lookup(equal, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  bm = idx.lookup(not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
  MESSAGE("subset lookup (in)");
  auto x = unbox(to<subnet>("192.168.0.0/23"));
  bm = idx.lookup(in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "111100");
  x = unbox(to<subnet>("192.168.0.0/25"));
  bm = idx.lookup(in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  MESSAGE("subset lookup (ni)");
  bm = idx.lookup(ni, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.168.1.128/25"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  x = unbox(to<subnet>("192.168.0.254/32"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.0.0.0/8"));
  bm = idx.lookup(ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  auto xs = list{s0, s1};
  auto multi = unbox(idx.lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "111100");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  subnet_index idx2{subnet_type{}};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  bm = idx2.lookup(not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
}
