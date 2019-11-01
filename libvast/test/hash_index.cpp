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

#define SUITE hash_index

#include "vast/hash_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(string) {
  factory<value_index>::initialize();
  hash_index idx{string_type{}, 5};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("bar")));
  REQUIRE(idx.append(make_data_view("baz")));
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view(caf::none)));
  REQUIRE(idx.append(make_data_view("bar"), 8));
  REQUIRE(idx.append(make_data_view("foo"), 9));
  MESSAGE("lookup");
  auto result = idx.lookup(equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "1001000001");
}

// The #id attribute selects the hash_index implementation.
TEST(value_index) {
  auto t = string_type{}.attributes({{"id"}});
  auto idx = factory<value_index>::make(t);
  auto ptr = dynamic_cast<hash_index*>(idx.get());
  CHECK(ptr != nullptr);
}
