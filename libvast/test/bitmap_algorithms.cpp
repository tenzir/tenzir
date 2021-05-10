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
