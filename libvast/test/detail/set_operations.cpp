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

#define SUITE set_operations
#include "test.hpp"

#include "vast/detail/set_operations.hpp"

using namespace vast::detail;

namespace {

struct fixture {
  fixture() {
    xs = {1, 2, 3, 6, 8, 9};
    ys = {2, 4, 6, 7};
    intersection = {2, 6};
    unification = {1, 2, 3, 4, 6, 7, 8, 9};
  }

  std::vector<int> xs;
  std::vector<int> ys;
  std::vector<int> intersection;
  std::vector<int> unification;
};

} // namespace <anonymous>

FIXTURE_SCOPE(set_operations_tests, fixture)

TEST(intersect) {
  auto result = intersect(xs, ys);
  CHECK_EQUAL(result, intersection);
}

TEST(inplace_intersect) {
  auto result = xs;
  inplace_intersect(result, ys);
  CHECK_EQUAL(result, intersection);
}

TEST(unify) {
  auto result = unify(xs, ys);
  CHECK_EQUAL(result, unification);
}

TEST(inplace_unify) {
  auto result = xs;
  inplace_unify(result, ys);
  CHECK_EQUAL(result, unification);
}

FIXTURE_SCOPE_END()
