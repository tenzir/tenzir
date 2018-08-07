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

#include <string>
#include <string_view>

#include "vast/detail/steady_map.hpp"

#define SUITE detail
#include "test.hpp"

#include "vast/config.hpp"

using namespace std::string_view_literals;
using namespace vast;

namespace {

using map = detail::steady_map<std::string, int>;

struct fixture {
  fixture() {
    xs.insert({"foo", 42});
    xs["baz"] = 1337;
    xs.emplace("bar", 4711);
  }

  map xs;
};

} // namespace <anonymous>

FIXTURE_SCOPE(steady_map_tests, fixture)

TEST(steady_map membership) {
  CHECK(xs.find("qux") == xs.end());
  CHECK(xs.find("foo") != xs.end());
  CHECK_EQUAL(xs.count("baz"), 1u);
}

#ifndef VAST_NO_EXCEPTIONS
TEST(steady_map at) {
  CHECK_EQUAL(xs.at("foo"), 42);
  auto exception = std::out_of_range{""};
  try {
    xs.at("qux");
  } catch (std::out_of_range& e) {
    exception = std::move(e);
  }
  CHECK_EQUAL(exception.what(), "vast::detail::vector_map::at out of range"sv);
}
#endif // VAST_NO_EXCEPTIONS

TEST(steady_map insert) {
  auto i = xs.insert({"qux", 1});
  CHECK(i.second);
  CHECK_EQUAL(i.first->second, 1);
  CHECK_EQUAL(xs.size(), 4u);
}

TEST(steady_map duplicates) {
  auto i = xs.insert({"foo", 666});
  CHECK(!i.second);
  CHECK_EQUAL(i.first->second, 42);
  CHECK_EQUAL(xs.size(), 3u);
}

TEST(steady_map erase) {
  CHECK_EQUAL(xs.erase("qux"), 0u);
  CHECK_EQUAL(xs.erase("baz"), 1u);
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs.begin()->second, 42);
  CHECK_EQUAL(xs.rbegin()->second, 4711);
  auto last = xs.erase(xs.begin());
  REQUIRE(last < xs.end());
  CHECK_EQUAL(last->first, "bar");
}

TEST(steady_map comparison) {
  CHECK(xs == map{{"foo", 42}, {"baz", 1337}, {"bar", 4711}});
  CHECK(xs != map{{"foo", 42}, {"bar", 4711}, {"baz", 1337}});
}

FIXTURE_SCOPE_END()
