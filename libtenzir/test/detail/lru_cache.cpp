//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/lru_cache.hpp"

#include "tenzir/test/test.hpp"

#include <string>

struct int_factory {
  auto operator()(int x) -> int {
    return x;
  }
};

struct string_factory {
  auto operator()(std::string const& x) -> int {
    return static_cast<int>(x.size());
  }
};

TEST("inserting and dropping") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  CHECK_EQUAL(cache.size(), 0u);
  cache.put(0, 0);
  cache.put(1, 1);
  cache.put(2, 2);
  CHECK_EQUAL(cache.size(), 3u);
  cache.put(3, 3);
  CHECK_EQUAL(cache.size(), 3u);
  size_t sum = 0;
  for (auto x : cache) {
    sum += x.second;
  }
  CHECK_EQUAL(sum, 6u);
  cache.drop(2);
  cache.drop(3);
  cache.drop(1);
  CHECK_EQUAL(cache.size(), 0u);
}

TEST("lru cache overriding") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.get_or_load(0);
  cache.get_or_load(1);
  cache.get_or_load(2);
  cache.put(1, 42);
  CHECK_EQUAL(cache.get_or_load(1), 42);
}

TEST("resizing") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.get_or_load(0);
  cache.get_or_load(1);
  cache.get_or_load(2);
  CHECK_EQUAL(cache.size(), 3u);
  cache.resize(1);
  CHECK_EQUAL(cache.size(), 1u);
  CHECK_EQUAL(cache.begin()->first,
              2); // Verify the oldest elements were erased.
  cache.resize(0);
  CHECK_EQUAL(cache.size(), 0u);
}

TEST("eject") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.put(1, 42);
  CHECK_EQUAL(cache.size(), size_t{1});
  auto x0 = cache.eject(0);
  CHECK_EQUAL(x0, 0);
  CHECK_EQUAL(cache.size(), size_t{1});
  auto x1 = cache.eject(1);
  CHECK_EQUAL(x1, 42);
  CHECK_EQUAL(cache.size(), size_t{0});
}

TEST("get reorders without loading") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(3, 3);
  auto* value = cache.get(1);
  REQUIRE_NOT_EQUAL(value, nullptr);
  CHECK_EQUAL(*value, 1);
  cache.put(4, 4);
  CHECK(cache.contains(1));
  CHECK(not cache.contains(2));
}

TEST("peek does not reorder") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.put(1, 1);
  cache.put(2, 2);
  cache.put(3, 3);
  auto* value = cache.peek(1);
  REQUIRE_NOT_EQUAL(value, nullptr);
  CHECK_EQUAL(*value, 1);
  cache.put(4, 4);
  CHECK(not cache.contains(1));
  CHECK(cache.contains(2));
}

TEST("string keys remain reachable after put") {
  tenzir::detail::lru_cache<std::string, int, string_factory> cache(
    2, string_factory{});
  cache.put("alpha", 1);
  cache.put("beta", 2);
  auto* alpha = cache.peek("alpha");
  auto* beta = cache.peek("beta");
  REQUIRE_NOT_EQUAL(alpha, nullptr);
  REQUIRE_NOT_EQUAL(beta, nullptr);
  CHECK_EQUAL(*alpha, 1);
  CHECK_EQUAL(*beta, 2);
}

TEST("zero-sized caches behave as no-store caches") {
  tenzir::detail::lru_cache<int, int, int_factory> cache(0, int_factory{});
  auto& loaded = cache.get_or_load(42);
  CHECK_EQUAL(loaded, 42);
  CHECK_EQUAL(cache.size(), size_t{0});
  CHECK(not cache.contains(42));
  CHECK_EQUAL(cache.peek(42), nullptr);
  auto& inserted = cache.put(7, 11);
  CHECK_EQUAL(inserted, 11);
  CHECK_EQUAL(cache.size(), size_t{0});
  CHECK(not cache.contains(7));
  CHECK_EQUAL(cache.peek(7), nullptr);
}
