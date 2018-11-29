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

#define SUITE algorithms

#include "vast/detail/algorithms.hpp"

#include "vast/test/test.hpp"

#include <map>
#include <vector>

using vast::detail::unique_values;

using imap = std::map<int, int>;

using ivec = std::vector<int>;

TEST(empty collection values) {
  CHECK_EQUAL(unique_values(imap()), ivec());
}

TEST(unique collection values) {
  CHECK_EQUAL(unique_values(imap({{1, 10}, {2, 30}, {3, 30}})), ivec({10, 30}));
}
