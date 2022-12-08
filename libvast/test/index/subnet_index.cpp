//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/index/subnet_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(subnet) {
  subnet_index idx{type{subnet_type{}}};
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
  auto bm = idx.lookup(relational_operator::ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.0.1"));
  bm = idx.lookup(relational_operator::ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  a = unbox(to<address>("192.168.1.42"));
  bm = idx.lookup(relational_operator::ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  // IPv6
  a = unbox(to<address>("feff::")); // too far out
  bm = idx.lookup(relational_operator::ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  a = unbox(to<address>("fe80::aaaa"));
  bm = idx.lookup(relational_operator::ni, make_data_view(a));
  CHECK_EQUAL(to_string(unbox(bm)), "000011");
  MESSAGE("equality lookup");
  bm = idx.lookup(relational_operator::equal, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  bm = idx.lookup(relational_operator::not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
  MESSAGE("subset lookup (in)");
  auto x = unbox(to<subnet>("192.168.0.0/23"));
  bm = idx.lookup(relational_operator::in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "111100");
  x = unbox(to<subnet>("192.168.0.0/25"));
  bm = idx.lookup(relational_operator::in, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  MESSAGE("subset lookup (ni)");
  bm = idx.lookup(relational_operator::ni, make_data_view(s0));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.168.1.128/25"));
  bm = idx.lookup(relational_operator::ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "010000");
  x = unbox(to<subnet>("192.168.0.254/32"));
  bm = idx.lookup(relational_operator::ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "101100");
  x = unbox(to<subnet>("192.0.0.0/8"));
  bm = idx.lookup(relational_operator::ni, make_data_view(x));
  CHECK_EQUAL(to_string(unbox(bm)), "000000");
  auto xs = list{s0, s1};
  auto multi = unbox(idx.lookup(relational_operator::in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "111100");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  subnet_index idx2{type{subnet_type{}}};
  CHECK_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  bm = idx2.lookup(relational_operator::not_equal, make_data_view(s1));
  CHECK_EQUAL(to_string(unbox(bm)), "101111");
}
