//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/stable_map.hpp"

#include <string>
#include <string_view>

#define SUITE detail
#include "vast/config.hpp"
#include "vast/test/test.hpp"

using namespace std::string_view_literals;
using namespace vast;

namespace {

struct fixture {
  fixture() {
    xs.insert({"foo", 42});
    xs["baz"] = 1337;
    xs.emplace("bar", 4711);
  }

  detail::stable_map<std::string, int> xs = {};
};

} // namespace

FIXTURE_SCOPE(stable_map_tests, fixture)

TEST(stable_map membership) {
  CHECK(!xs.contains("qux"));
  CHECK(xs.find("foo") != xs.end());
  CHECK_EQUAL(xs.count("baz"), 1u);
}

TEST(stable_map at) {
  CHECK_EQUAL(xs.at("foo"), 42);
  auto exception = std::out_of_range{""};
  try {
    [[maybe_unused]] auto _ = xs.at("qux");
  } catch (std::out_of_range& e) {
    exception = std::move(e);
  }
  CHECK_EQUAL(exception.what(), "vast::detail::vector_map::at out of range"sv);
}

TEST(stable_map insert) {
  xs.clear();
  // Insert 4 elements in non-sorted order. The numbers
  auto i1 = xs.insert({"qux", 3});
  CHECK(i1.second);
  auto i2 = xs.insert({"ax", 0});
  CHECK(i2.second);
  auto i3 = xs.insert({"erx", 1});
  CHECK(i3.second);
  auto i4 = xs.insert({"qtp", 2});
  CHECK(i4.second);
  // Check map content.
  CHECK_EQUAL(xs.size(), 4u);
  CHECK_EQUAL(xs["ax"], 0);
  CHECK_EQUAL(xs["erx"], 1);
  CHECK_EQUAL(xs["qtp"], 2);
  CHECK_EQUAL(xs["qux"], 3);
  // Check that the underlying data is stored in the order it was inserted.
  std::vector<int> insert_order{3, 0, 1, 2};
  auto& vec = as_vector(xs);
  for (size_t i = 0; i < xs.size(); ++i) {
    CHECK_EQUAL(vec.at(i).second, insert_order.at(i));
  }
}

TEST(stable_map duplicates) {
  auto i = xs.insert({"foo", 666});
  CHECK(!i.second);
  CHECK_EQUAL(i.first->second, 42);
  CHECK_EQUAL(xs.size(), 3u);
}

TEST(stable_map erase) {
  CHECK_EQUAL(xs.erase("qux"), 0u);
  CHECK_EQUAL(xs.erase("baz"), 1u);
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs.begin()->second, 42);
  CHECK_EQUAL(xs.rbegin()->second, 4711);
  auto last = xs.erase(xs.begin());
  REQUIRE(last < xs.end());
  CHECK_EQUAL(last->first, "bar");
}

TEST(stable_map comparison) {
  using legacy_map_type = decltype(xs);
  CHECK((xs == legacy_map_type{{"foo", 42}, {"baz", 1337}, {"bar", 4711}}));
  CHECK((xs != legacy_map_type{{"foo", 42}, {"bar", 4711}, {"baz", 1337}}));
}

FIXTURE_SCOPE_END()
