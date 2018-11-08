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

#include "vast/offset.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/offset.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/offset.hpp"

#include "vast/test/test.hpp"

using namespace vast;

TEST(offset printing) {
  auto o = offset{0, 10, 8};
  CHECK(to_string(o) == "0,10,8");
}

TEST(offset parsing) {
  auto o = to<offset>("0,4,8,12");
  CHECK(o);
  CHECK(*o == offset({0, 4, 8, 12}));
}
