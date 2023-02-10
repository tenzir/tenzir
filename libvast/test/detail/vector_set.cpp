//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/flat_set.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace vast::detail;

namespace {

template <class Set>
struct fixture {
  fixture() {
    xs = {1, 2, 8, 3, 7};
  }

  void test() {
    MESSAGE("find");
    CHECK(!xs.contains(0));
    CHECK(xs.contains(1));
    CHECK(xs.find(2) != xs.end());
    CHECK(xs.find(4) == xs.end());
    CHECK_EQUAL(xs.count(8), 1u);
    MESSAGE("erase");
    CHECK_EQUAL(xs.erase(0), 0u);
    CHECK_EQUAL(xs.erase(2), 1u);
    CHECK(xs.find(2) == xs.end());
    auto next = xs.erase(xs.begin());
    REQUIRE(next != xs.end());
    CHECK(xs.find(1) == xs.end());
    CHECK_EQUAL(xs.size(), 3u);
    MESSAGE("insert duplicate");
    auto i = xs.insert(7);
    CHECK(!i.second);
    CHECK_EQUAL(*i.first, 7);
    MESSAGE("insert new");
    i = xs.insert(0);
    CHECK(i.second);
    CHECK_EQUAL(*i.first, 0);
    i = xs.insert(4);
    CHECK(i.second);
    CHECK_EQUAL(*i.first, 4);
    CHECK_EQUAL(xs.size(), 5u);
  }

  Set xs;
};

} // namespace

FIXTURE_SCOPE(stable_set_tests, fixture<stable_set<int>>)

TEST(stable_set) {
  test();
}

TEST(stable_set comparison) {
  auto xs = stable_set<int>{1, 2, 3};
  auto ys = stable_set<int>{2, 1, 3};
  CHECK_NOT_EQUAL(xs, ys);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(flat_set_tests, fixture<flat_set<int>>)

TEST(flat_set) {
  test();
}

TEST(flat_set comparison) {
  auto xs = flat_set<int>{1, 2, 3};
  auto ys = flat_set<int>{2, 1, 3};
  CHECK_EQUAL(xs, ys);
}

FIXTURE_SCOPE_END()
