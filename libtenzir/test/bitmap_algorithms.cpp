//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bitmap_algorithms.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("is subset") {
  CHECK(is_subset(make_ids({{10, 20}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{11, 20}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{10, 19}}), make_ids({{10, 20}})));
  CHECK(is_subset(make_ids({{10, 19}}), make_ids({{10, 20}})));
}

TEST("is not subset") {
  CHECK(!is_subset(make_ids({{9, 19}}), make_ids({{10, 20}})));
  CHECK(!is_subset(make_ids({{11, 21}}), make_ids({{10, 20}})));
  CHECK(!is_subset(make_ids({5, 15, 25}), make_ids({{10, 20}})));
}

TEST("bitwise_range select") {
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

TEST("select runs") {
  auto bm = make_ids({{0, 1}, {50000, 50001}, {100000, 100003}});
  {
    auto ranges = collect(select_runs(bm));
    REQUIRE_EQUAL(ranges.size(), 3u);
    CHECK_EQUAL(ranges[0].first, 0u);
    CHECK_EQUAL(ranges[0].last, 1u);
    CHECK_EQUAL(ranges[1].first, 50000u);
    CHECK_EQUAL(ranges[1].last, 50001u);
    CHECK_EQUAL(ranges[2].first, 100000u);
    CHECK_EQUAL(ranges[2].last, 100003u);
  }
  {
    auto ranges = collect(select_runs<0>(bm));
    REQUIRE_EQUAL(ranges.size(), 2u);
    CHECK_EQUAL(ranges[0].first, 1u);
    CHECK_EQUAL(ranges[0].last, 50000u);
    CHECK_EQUAL(ranges[1].first, 50001u);
    CHECK_EQUAL(ranges[1].last, 100000u);
  }
  bm.append_bit(false);
  {
    auto ranges = collect(select_runs(bm));
    REQUIRE_EQUAL(ranges.size(), 3u);
    CHECK_EQUAL(ranges[0].first, 0u);
    CHECK_EQUAL(ranges[0].last, 1u);
    CHECK_EQUAL(ranges[1].first, 50000u);
    CHECK_EQUAL(ranges[1].last, 50001u);
    CHECK_EQUAL(ranges[2].first, 100000u);
    CHECK_EQUAL(ranges[2].last, 100003u);
  }
  {
    auto ranges = collect(select_runs<0>(bm));
    REQUIRE_EQUAL(ranges.size(), 3u);
    CHECK_EQUAL(ranges[0].first, 1u);
    CHECK_EQUAL(ranges[0].last, 50000u);
    CHECK_EQUAL(ranges[1].first, 50001u);
    CHECK_EQUAL(ranges[1].last, 100000u);
    CHECK_EQUAL(ranges[2].first, 100003u);
    CHECK_EQUAL(ranges[2].last, 100004u);
  }
}
