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

#define SUITE column_iterator

#include "vast/detail/column_iterator.hpp"

#include "vast/test/test.hpp"

#include <numeric>

#include "vast/detail/range.hpp"

using namespace vast;

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

} // namespace <anonymous>

#define CHECK_RANGE(rng, ...)                                                  \
  {                                                                            \
    std::vector<int> xs{rng.begin(), rng.end()};                               \
    std::vector<int> ys{__VA_ARGS__};                                          \
    CHECK_EQUAL(xs, ys);                                                       \
  }

FIXTURE_SCOPE(column_iterator_tests, fixture)

TEST(four by four) {
  MESSAGE("visit buf as if 4x4 matrix");
  CHECK_RANGE(column(4, 0), 0, 4, 8, 12);
  CHECK_RANGE(column(4, 1), 1, 5, 9, 13);
  CHECK_RANGE(column(4, 2), 2, 6, 10, 14);
  CHECK_RANGE(column(4, 3), 3, 7, 11, 15);
}

TEST(two by eight) {
  MESSAGE("visit buf as if 2x8 matrix");
  CHECK_RANGE(column(2, 0), 0, 2, 4, 6, 8, 10, 12, 14);
  CHECK_RANGE(column(2, 1), 1, 3, 5, 7, 9, 11, 13, 15);
}

TEST(eight by two) {
  MESSAGE("visit buf as if 8x2 matrix");
  CHECK_RANGE(column(8, 0), 0, 8);
  CHECK_RANGE(column(8, 1), 1, 9);
  CHECK_RANGE(column(8, 2), 2, 10);
  CHECK_RANGE(column(8, 3), 3, 11);
  CHECK_RANGE(column(8, 4), 4, 12);
  CHECK_RANGE(column(8, 5), 5, 13);
  CHECK_RANGE(column(8, 6), 6, 14);
  CHECK_RANGE(column(8, 7), 7, 15);
}

FIXTURE_SCOPE_END()
