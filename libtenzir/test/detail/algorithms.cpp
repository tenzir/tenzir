//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/algorithms.hpp"

#include "tenzir/test/test.hpp"

#include <map>
#include <vector>

using tenzir::detail::contains;
using tenzir::detail::unique_values;

using imap = std::map<int, int>;

using iset = std::set<int>;

using ivec = std::vector<int>;

TEST("contains") {
  CHECK(contains(iset({1, 2, 3, 4}), 2));
  CHECK(contains(ivec({1, 2, 3, 4}), 2));
  CHECK(!contains(iset({1, 2, 3, 4}), 5));
  CHECK(!contains(ivec({1, 2, 3, 4}), 5));
}

TEST("empty collection values") {
  CHECK_EQUAL(unique_values(imap()), ivec());
}

TEST("unique collection values") {
  CHECK_EQUAL(unique_values(imap({{1, 10}, {2, 30}, {3, 30}})), ivec({10, 30}));
}
