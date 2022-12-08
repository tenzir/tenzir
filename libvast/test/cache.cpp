//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE detail

#include "vast/detail/cache.hpp"

#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/test/test.hpp"

using namespace vast;

namespace {

template <class Policy>
struct fixture {
  fixture() {
    CHECK(xs.emplace("foo", 1).second);
    CHECK(xs.emplace("bar", 2).second);
    CHECK(xs.emplace("baz", 3).second);
    CHECK(xs.emplace("qux", 4).second);
  }

  detail::cache<std::string, int, Policy> xs;
};

} // namespace

FIXTURE_SCOPE(lru_cache_tests, fixture<detail::lru>)

TEST(LRU cache lookup) {
  auto i = xs.find("bar");
  REQUIRE(i != xs.end());
  CHECK_EQUAL(i->first, "bar");
  CHECK_EQUAL(i->second, 2);
  // Element has been moved to the back.
  CHECK_EQUAL(*i, *xs.rbegin());
}

TEST(LRU cache eviction) {
  auto i = 0;
  xs.on_evict([&](std::string&, int x) { i = x; });
  xs.evict();
  CHECK_EQUAL(i, 1);
}

TEST(LRU cache shrinking) {
  CHECK_EQUAL(xs.size(), 4u);
  xs.capacity(1);
  CHECK_EQUAL(xs.size(), 1u);
}

TEST(LRU cache insertion) {
  xs.capacity(4);
  auto i = xs.emplace("qux", 42); // element exists
  CHECK(!i.second);
  CHECK_EQUAL(i.first->first, "qux");
  CHECK_EQUAL(i.first->second, 4);
  i = xs.emplace("new", 42);
  CHECK(i.second);
  CHECK_EQUAL(i.first->first, "new");
  CHECK_EQUAL(i.first->second, 42);
  // least recently used element is gone
  auto j = xs.find("foo");
  CHECK(j == xs.end());
  CHECK_EQUAL(xs.size(), 4u);
}

TEST(cache serialization) {
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, xs));
  decltype(xs) ys;
  CHECK_EQUAL(detail::legacy_deserialize(buf, ys), true);
  CHECK(xs == ys);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(mru_cache_tests, fixture<detail::mru>)

TEST(MRU cache lookup) {
  auto i = xs.find("bar");
  REQUIRE(i != xs.end());
  // Element has been moved to the front.
  CHECK_EQUAL(i, xs.begin());
}

TEST(MRU cache eviction) {
  xs.on_evict([&](std::string&, int v) { CHECK_EQUAL(v, 4); });
  xs.evict();
}

TEST(MRU cache insertion) {
  xs.capacity(4);
  auto i = xs.emplace("new", 42);
  CHECK(i.second);
  CHECK_EQUAL(i.first->first, "new");
  CHECK_EQUAL(i.first->second, 42);
  // most recently used element is gone
  auto j = xs.find("qux");
  CHECK(j == xs.end());
  CHECK_EQUAL(xs.size(), 4u);
}

FIXTURE_SCOPE_END()
