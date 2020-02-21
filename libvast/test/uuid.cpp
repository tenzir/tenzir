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

#define SUITE uuid

#include "vast/uuid.hpp"

#include "vast/test/test.hpp"

#include "vast/byte.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"

using namespace vast;

TEST(pod size) {
  CHECK(sizeof(uuid) == size_t{16});
}

TEST(parseable and printable) {
  auto x = to<uuid>("01234567-89ab-cdef-0123-456789abcdef");
  REQUIRE(x);
  CHECK_EQUAL(to_string(*x), "01234567-89ab-cdef-0123-456789abcdef");
}

TEST(construction from span) {
  std::array<char, 16> bytes{0, 1, 2,  3,  4,  5,  6,  7,
                             8, 9, 10, 12, 12, 13, 14, 15};
  auto bytes_view = as_bytes(span<char, 16>{bytes});
  CHECK(bytes_view == as_bytes(uuid{bytes_view}));
}
