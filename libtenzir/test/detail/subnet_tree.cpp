//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/subnet_tree.hpp"

#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/collect.hpp"

using namespace tenzir;
using namespace tenzir::detail;

TEST("prefix matching") {
  auto xs = detail::subnet_tree{};
  auto sn_0_24 = *to<subnet>("192.168.0.0/24");
  auto sn_0_25 = *to<subnet>("192.168.0.0/25");
  auto sn_1_24 = *to<subnet>("192.168.1.0/24");
  auto sn_0_23 = *to<subnet>("192.168.0.0/23");
  CHECK(xs.insert(sn_0_24, 0u));
  CHECK(xs.insert(sn_0_25, 1u));
  CHECK(xs.insert(sn_1_24, 2u));
  CHECK(xs.insert(sn_0_23, 3u));
  CHECK(not xs.insert(sn_0_23, 3u)); // duplicate
  // Check for true positives.
  CHECK(xs.lookup(sn_0_24));
  CHECK(xs.lookup(sn_0_25));
  CHECK(xs.lookup(sn_1_24));
  CHECK(xs.lookup(sn_0_23));
  CHECK(xs.match(*to<ip>("192.168.0.1")).second);
  CHECK(xs.match(*to<ip>("192.168.1.255")).second);
  {
    const auto ptr = xs.match(*to<subnet>("192.168.0.128/25"));
    REQUIRE(ptr.second);
    CHECK_EQUAL(*ptr.second, data{0u});
  }
  {
    const auto ptr = xs.match(*to<subnet>("192.168.0.0/25"));
    REQUIRE(ptr.second);
    CHECK_EQUAL(*ptr.second, data{1u});
  }
  // Check for true negatives.
  CHECK(not xs.lookup(*to<subnet>("192.168.0.0/22")));
  CHECK(not xs.match(*to<ip>("192.168.2.0")).second);
  CHECK(not xs.match(*to<ip>("10.0.0.1")).second);
  // Prefix match of IP addresses.
  auto subnets = std::set<subnet>{};
  for (auto [sn, _] : xs.search(*to<ip>("192.168.0.1")))
    subnets.insert(sn);
  auto expected = std::set{sn_0_24, sn_0_25, sn_0_23};
  CHECK_EQUAL(subnets, expected);
  // Remove one subnet.
  CHECK(xs.erase(sn_0_24));
  CHECK(not xs.erase(sn_0_24)); // not there anymore
  // Check what's remaining.
  auto ys = collect(xs.nodes());
  REQUIRE_EQUAL(ys.size(), 3u);
  CHECK_EQUAL(ys[0].first, sn_0_23);
  CHECK_EQUAL(ys[1].first, sn_0_25);
  CHECK_EQUAL(ys[2].first, sn_1_24);
}
