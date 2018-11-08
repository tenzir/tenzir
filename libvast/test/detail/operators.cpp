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

#define SUITE operators
#include "vast/test/test.hpp"

#include "vast/detail/operators.hpp"

using namespace vast::detail;

namespace {

struct foo : addable<foo>,
             addable<foo, int> {
  explicit foo(int x) : value{x} {
    // nop
  }

  foo& operator+=(const foo& other) {
    value += other.value;
    return *this;
  }

  foo& operator+=(int x) {
    value += x;
    return *this;
  }

  int value;
};

} // namespace <anonymous>

TEST(commutative operators) {
  auto x = foo{42};
  auto y = foo{-3};
  auto result = 1 + x + 1 + y + 1;
  CHECK_EQUAL(result.value, 42);
}
