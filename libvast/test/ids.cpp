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

#include "vast/ids.hpp"

#define SUITE ids
#include "vast/test/test.hpp"

using namespace vast;

TEST(make ids) {
  ids xs;
  xs.append_bit(false);
  xs.append_bit(true);
  xs.append_bit(true);
  xs.append_bits(false, 7);
  xs.append_bits(true, 10);
  auto ys = make_ids({1, 2, {10, 20}});
  CHECK_EQUAL(xs, ys);
  auto zs = make_ids({{15, 20}, 2, {10, 15}, 1});
  CHECK_EQUAL(ys, zs);
}
