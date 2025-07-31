//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/flat_map.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

namespace {

struct fixture {
  fixture() {
    xs[43] = 4.3;
    xs.insert({42, 4.2});
    xs.emplace(44, 4.4);
  }

  detail::flat_map<int, double> xs;
};

} // namespace

WITH_FIXTURE(fixture) {
  TEST("membership") {
    CHECK(xs.find(7) == xs.end());
    CHECK(xs.find(42) != xs.end());
    CHECK_EQUAL(xs.count(43), 1u);
  }

  TEST("lookup") {
    detail::flat_map<int, double> xs;
    xs[5] = 1;
    xs[4] = 1;
    xs[3] = 1;
    xs[2] = 1;
    xs[1] = 1;
    CHECK(xs.find(1) != xs.end());
    CHECK(xs.find(5) != xs.end());
    CHECK(xs.find(42) == xs.end());
    CHECK_EQUAL(xs.count(2), 1u);
  }

  TEST("insert") {
    auto i = xs.emplace(1, 3.14);
    CHECK(i.second);
    CHECK_EQUAL(i.first->first, 1);
    CHECK_EQUAL(i.first->second, 3.14);
    CHECK_EQUAL(xs.size(), 4u);
  }

  TEST("duplicates") {
    auto i = xs.emplace(42, 4.2);
    CHECK(! i.second);
    CHECK_EQUAL(i.first->second, 4.2);
    CHECK_EQUAL(xs.size(), 3u);
  }

  TEST("erase") {
    CHECK_EQUAL(xs.erase(1337), 0u);
    CHECK_EQUAL(xs.erase(42), 1u);
    REQUIRE_EQUAL(xs.size(), 2u);
    CHECK_EQUAL(xs.begin()->second, 4.3);
    CHECK_EQUAL(xs.rbegin()->second, 4.4);
    auto last = xs.erase(xs.begin());
    REQUIRE(last < xs.end());
    CHECK_EQUAL(last->first, 44);
  }
}
