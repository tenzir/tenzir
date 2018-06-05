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

#define SUITE lru_cache
#include "test.hpp"

#include "vast/detail/flat_lru_cache.hpp"

#include <string>

#include <caf/meta/type_name.hpp>

using std::literals::operator""sv;

namespace {

struct kvp {
  std::string key;
  int value;

  explicit kvp(std::string k, int v = 0) : key(std::move(k)), value(v) {
    // nop
  }
};

template <class Inspector>
auto inspect(Inspector& f, kvp& x) {
  return f(caf::meta::type_name("kvp"), x.key, x.value);
}

bool operator==(const kvp& x, const kvp& y) {
  return x.key == y.key && x.value == y.value;
}

struct has_key {
  auto operator()(std::string_view key) const {
    return [=](const kvp& x) { return x.key == key; };
  }
};

struct make_kvp {
  auto operator()(std::string_view key) const {
    return kvp{std::string{key}};
  }
};

struct fixture {
  fixture() : cache(5) {
    // nop
  }

  vast::detail::flat_lru_cache<kvp, has_key, make_kvp> cache;
};

} // namespace <anonymous>

FIXTURE_SCOPE(lru_cache_tests, fixture)

TEST(filling) {
  std::vector<kvp> expected{kvp{"one"}, kvp{"two"}, kvp{"three"}, kvp{"four"},
                            kvp{"five"}};
  for (auto key : {"one", "two", "three", "four", "five"})
    cache.get_or_add(key);
  CHECK_EQUAL(cache.elements(), expected);
}

TEST(overriding) {
  std::vector<kvp> expected{kvp{"three"}, kvp{"four"}, kvp{"five"}, kvp{"six"},
                            kvp{"seven"}};
  for (auto key : {"one", "two", "three", "four", "five", "six", "seven"})
    cache.get_or_add(key);
  CHECK_EQUAL(cache.elements(), expected);
}

TEST(reordering) {
  std::vector<kvp> expected{kvp{"one"}, kvp{"three"}, kvp{"four"}, kvp{"five"},
                            kvp{"two"}};
  for (auto key : {"one", "two", "three", "four", "five"})
    cache.get_or_add(key);
  cache.get_or_add("two");
  CHECK_EQUAL(cache.elements(), expected);
}

FIXTURE_SCOPE_END()
