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
#include "vast/load.hpp"
#include "vast/save.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(string) {
  // This one-byte parameterization creates a collision for "foo" and "bar".
  hash_index<1> idx{string_type{}};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view("bar")));
  REQUIRE(idx.append(make_data_view("baz")));
  REQUIRE(idx.append(make_data_view("foo")));
  REQUIRE(idx.append(make_data_view(caf::none)));
  REQUIRE(idx.append(make_data_view("bar"), 8));
  REQUIRE(idx.append(make_data_view("foo"), 9));
  REQUIRE(idx.append(make_data_view(caf::none)));
  MESSAGE("lookup");
  auto result = idx.lookup(equal, make_data_view("foo"));
  CHECK_EQUAL(to_string(unbox(result)), "10010000010");
  result = idx.lookup(not_equal, make_data_view("foo"));
  REQUIRE(result);
  CHECK_EQUAL(to_string(unbox(result)), "01101000101");
}

TEST(serialization) {
  hash_index<1> x{string_type{}};
  REQUIRE(x.append(make_data_view("foo")));
  REQUIRE(x.append(make_data_view("bar")));
  REQUIRE(x.append(make_data_view("baz")));
  std::vector<char> buf;
  REQUIRE(save(nullptr, buf, x) == caf::none);
  hash_index<1> y{string_type{}};
  REQUIRE(load(nullptr, buf, y) == caf::none);
  auto result = y.lookup(not_equal, make_data_view("bar"));
  CHECK_EQUAL(to_string(unbox(result)), "101");
  // Cannot append after deserialization.
  CHECK(!y.append(make_data_view("foo")));
}

// The attribute #index=hash selects the hash_index implementation.
TEST(value_index) {
  auto t = string_type{}.attributes({{"index", "hash"}});
  factory<value_index>::initialize();
  auto idx = factory<value_index>::make(t);
  // FIXME: we can't know the concrete the parameterization here. This test
  using concrete_type = hash_index<5>;
  auto ptr = dynamic_cast<concrete_type*>(idx.get());
  CHECK(ptr != nullptr);
  idx = factory<value_index>::make(string_type{});
  ptr = dynamic_cast<concrete_type*>(idx.get());
  CHECK(ptr == nullptr);
}
