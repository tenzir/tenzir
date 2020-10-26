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
#include "vast/detail/lru_cache.hpp"

#include "vast/test/test.hpp"

struct int_factory {
  int operator()(int x) {
    return x;
  }
};

TEST(inserting and dropping) {
  // Insert elements.
  vast::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  CHECK_EQUAL(cache.size(), 0u);
  cache.put(0, 0);
  cache.put(1, 1);
  cache.put(2, 2);
  CHECK_EQUAL(cache.size(), 3u);
  // Check that entering a fourth element dropped the first one.
  cache.put(3, 3);
  CHECK_EQUAL(cache.size(), 3u);
  size_t sum = 0;
  for (auto x : cache)
    sum += x.second;
  CHECK_EQUAL(sum, 6u);
  // Remove elements.
  cache.drop(2);
  cache.drop(3);
  cache.drop(1);
  CHECK_EQUAL(cache.size(), 0u);
}

TEST(overriding) {
  vast::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
  cache.get_or_load(0);
  cache.get_or_load(1);
  cache.get_or_load(2);
  cache.put(1, 42);
  CHECK_EQUAL(cache.get_or_load(1), 42);
}

TEST(resizing) {
  vast::detail::lru_cache<int, int, int_factory> cache(3, int_factory{});
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
