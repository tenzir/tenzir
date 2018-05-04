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

#include <string>

#include "vast/detail/byte.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/span.hpp"

#include "test.hpp"

using namespace std::string_literals;
using namespace vast::detail;

TEST(string span) {
  auto foo = "foo"s;
  auto x = span<char>{foo};
  CHECK_EQUAL(x.size(), 3);
}

TEST(byte span) {
  auto b = byte{0b0000'1100};
  auto x = span<byte>{&b, 1};
  CHECK_EQUAL(x.size(), 1);
  auto foo = "foo"s;
  x = span<byte>(reinterpret_cast<byte*>(foo.data()), foo.size());
  CHECK_EQUAL(x.size(), 3);
  CHECK_EQUAL(x[0], byte{'f'});
}
