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

#include "vast/detail/byte.hpp"

#define SUITE byte
#include "vast/test/test.hpp"

using namespace vast::detail;

TEST(bitwise operations) {
  auto x = byte{0b0000'1100};
  x >>= 2;
  CHECK_EQUAL(to_integer<int>(x), 0b0000'0011);
  x ^= byte{0b1010'1010};
  CHECK_EQUAL(to_integer<int>(x), 0b1010'1001);
}

TEST(to_byte) {
  auto x = to_byte<42>();
  CHECK_EQUAL(to_integer<int>(x), 42);
}
