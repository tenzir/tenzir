//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/set_operations.hpp"

#include "vast/test/test.hpp"

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

} // namespace

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
