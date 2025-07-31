//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/flat_lru_cache.hpp"

#include "tenzir/test/test.hpp"

#include <string>
#include <string_view>

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
  return f.apply(x.key) && f.apply(x.value);
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

  tenzir::detail::flat_lru_cache<kvp, has_key, make_kvp> cache;
};

} // namespace

WITH_FIXTURE(fixture) {
  TEST("filling") {
    std::vector<kvp> expected{kvp{"one"}, kvp{"two"}, kvp{"three"}, kvp{"four"},
                              kvp{"five"}};
    for (auto key : {"one", "two", "three", "four", "five"}) {
      cache.add(kvp{key});
    }
    CHECK_EQUAL(cache.elements(), expected);
  }

  TEST("overriding") {
    std::vector<kvp> expected{kvp{"three"}, kvp{"four"}, kvp{"five"},
                              kvp{"six"}, kvp{"seven"}};
    for (auto key : {"one", "two", "three", "four", "five", "six", "seven"}) {
      cache.add(kvp{key});
    }
    CHECK_EQUAL(cache.elements(), expected);
  }

  TEST("reordering") {
    std::vector<kvp> expected{kvp{"one"}, kvp{"three"}, kvp{"four"},
                              kvp{"five"}, kvp{"two"}};
    for (auto key : {"one", "two", "three", "four", "five"}) {
      cache.add(kvp{key});
    }
    cache.get_or_add("two");
    CHECK_EQUAL(cache.elements(), expected);
  }
}
