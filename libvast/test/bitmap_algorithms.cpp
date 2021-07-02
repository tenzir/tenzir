//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bitmap_algorithms

#include "vast/bitmap_algorithms.hpp"

#include "vast/ids.hpp"
#include "vast/test/test.hpp"

using namespace vast;

TEST(is subset) {
  CHECK(is_subset(make_ids({{10, 20}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{11, 20}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{10, 19}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{10, 19}}), make_ids({{10, 20}})));
}

TEST(is not subset) {
  CHECK(!is_subset(make_ids({{9, 19}}), make_ids({{10, 20}})));
  CHECK(!is_subset(make_ids({{11, 21}}), make_ids({{10, 20}})));
  CHECK(!is_subset(make_ids({5, 15, 25}), make_ids({{10, 20}})));
}

TEST(bitwise_range select) {
  auto bm = make_ids({{0, 1}, {50000, 50001}, {100000, 100003}});
  CHECK_EQUAL(rank(bm), 5ull);
  auto rng0 = each(bm);
  CHECK_EQUAL(rng0.get(), 0ull);
  auto rng1 = each(bm);
  rng1.select(1);
  CHECK_EQUAL(rng1.get(), 50000ull);
  auto rng2 = each(bm);
  rng2.select(2);
  CHECK_EQUAL(rng2.get(), 100000ull);
  auto rng3 = each(bm);
  rng3.select(3);
  CHECK_EQUAL(rng3.get(), 100001ull);
}
