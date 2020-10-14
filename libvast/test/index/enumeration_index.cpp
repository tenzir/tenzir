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

#define SUITE value_index

#include "vast/index/enumeration_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  fixture() {
    factory<value_index>::initialize();
  }
};

} // namespace

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(enumeration) {
  auto e = enumeration_type{{"foo", "bar"}};
  auto idx = enumeration_index(e);
  REQUIRE(idx.append(enumeration{0}));
  REQUIRE(idx.append(enumeration{0}));
  REQUIRE(idx.append(enumeration{1}));
  REQUIRE(idx.append(enumeration{0}));
  auto foo = idx.lookup(equal, make_data_view(enumeration{0}));
  CHECK_EQUAL(to_string(foo), "1101");
  auto bar = idx.lookup(not_equal, make_data_view(enumeration{0}));
  CHECK_EQUAL(to_string(bar), "0010");
}

FIXTURE_SCOPE_END()
