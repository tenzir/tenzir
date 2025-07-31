//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/column_iterator.hpp"

#include "tenzir/detail/range.hpp"
#include "tenzir/test/test.hpp"

#include <numeric>

using namespace tenzir;

namespace {

using column_range = detail::iterator_range<detail::column_iterator<int>>;

struct fixture {
  std::vector<int> buf;

  fixture() {
    buf.resize(16);
    std::iota(buf.begin(), buf.end(), 0);
  }

  column_range column(size_t columns, size_t col) {
    auto rows = 16 / columns;
    detail::column_iterator<int> first{buf.data() + col, columns};
    return {first, first + rows};
  }
};

} // namespace

#define CHECK_RANGE(rng, ...)                                                  \
  {                                                                            \
    std::vector<int> xs{rng.begin(), rng.end()};                               \
    std::vector<int> ys{__VA_ARGS__};                                          \
    CHECK_EQUAL(xs, ys);                                                       \
  }

WITH_FIXTURE(fixture) {
  TEST("four by four") {
    MESSAGE("visit buf as 4x4 matrix");
    CHECK_RANGE(column(4, 0), 0, 4, 8, 12);
    CHECK_RANGE(column(4, 1), 1, 5, 9, 13);
    CHECK_RANGE(column(4, 2), 2, 6, 10, 14);
    CHECK_RANGE(column(4, 3), 3, 7, 11, 15);
  }

  TEST("two by eight") {
    MESSAGE("visit buf as 2x8 matrix");
    CHECK_RANGE(column(2, 0), 0, 2, 4, 6, 8, 10, 12, 14);
    CHECK_RANGE(column(2, 1), 1, 3, 5, 7, 9, 11, 13, 15);
  }

  TEST("eight by two") {
    MESSAGE("visit buf as 8x2 matrix");
    CHECK_RANGE(column(8, 0), 0, 8);
    CHECK_RANGE(column(8, 1), 1, 9);
    CHECK_RANGE(column(8, 2), 2, 10);
    CHECK_RANGE(column(8, 3), 3, 11);
    CHECK_RANGE(column(8, 4), 4, 12);
    CHECK_RANGE(column(8, 5), 5, 13);
    CHECK_RANGE(column(8, 6), 6, 14);
    CHECK_RANGE(column(8, 7), 7, 15);
  }
}
