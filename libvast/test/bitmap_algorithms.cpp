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
