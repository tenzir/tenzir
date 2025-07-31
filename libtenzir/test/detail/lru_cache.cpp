//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/lru_cache.hpp"

#include "tenzir/test/test.hpp"

struct int_factory {
  int operator()(int x) {
    return x;
  }
};

TEST("inserting and dropping") {
  // Insert elements.
  tenzir::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  CHECK_EQUAL(cache.size(), 0u);
  cache.put(0, 0);
  cache.put(1, 1);
  cache.put(2, 2);
  CHECK_EQUAL(cache.size(), 3u);
  // Check that entering a fourth element dropped the first one.
  cache.put(3, 3);
  CHECK_EQUAL(cache.size(), 3u);
  size_t sum = 0;
  for (auto x : cache) {
    sum += x.second;
  }
  CHECK_EQUAL(sum, 6u);
  // Remove elements.
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
