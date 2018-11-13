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

#include <unordered_set>

#include "vast/uuid.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"

#include "vast/test/test.hpp"

using namespace vast;

TEST(UUID) {
  CHECK(sizeof(uuid) == 16ul);
  auto u = to<uuid>("01234567-89ab-cdef-0123-456789abcdef");
  REQUIRE(u);
  CHECK(to_string(*u) == "01234567-89ab-cdef-0123-456789abcdef");

  std::unordered_set<uuid> set;
  set.insert(*u);
  set.insert(uuid::random());
  set.insert(uuid::random());
  CHECK(set.find(*u) != set.end());
}
