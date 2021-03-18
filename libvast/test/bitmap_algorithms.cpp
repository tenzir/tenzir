// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE bitmap_algorithms

#include "vast/bitmap_algorithms.hpp"

#include "vast/test/test.hpp"

#include "vast/ids.hpp"

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
