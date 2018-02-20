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

#include "vast/detail/flat_set.hpp"
#include "vast/detail/steady_set.hpp"

#define SUITE detail
#include "test.hpp"

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
    CHECK(xs.find(0) == xs.end());
    CHECK(xs.find(1) != xs.end());
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

} // namespace <anonymous>

FIXTURE_SCOPE(steady_set_tests, fixture<steady_set<int>>)

TEST(steady_set) {
  test();
}

TEST(steady_set comparison) {
  auto xs = steady_set<int>{1, 2, 3};
  auto ys = steady_set<int>{2, 1, 3};
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
