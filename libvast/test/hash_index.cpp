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
#include "vast/si_literals.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;
using namespace vast::si_literals;

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
TEST(factory construction and parameterization) {
  factory<value_index>::initialize();
  auto t = string_type{}.attributes({{"index", "hash"}});
  caf::settings opts;
  MESSAGE("test cardinality that is a power of 2");
  opts["cardinality"] = 1_Ki;
  auto idx = factory<value_index>::make(t, opts);
  auto ptr3 = dynamic_cast<hash_index<3>*>(idx.get()); // 20 bits in 3 bytes
  CHECK(ptr3 != nullptr);
  CHECK_EQUAL(idx->options().size(), 1u);
  MESSAGE("test cardinality that is not a power of 2");
  opts["cardinality"] = 1_Mi + 7;
  idx = factory<value_index>::make(t, opts);
  auto ptr6 = dynamic_cast<hash_index<6>*>(idx.get()); // 41 bits in 6 bytes
  CHECK(ptr6 != nullptr);
  MESSAGE("no options");
  idx = factory<value_index>::make(t, caf::settings{});
  auto ptr5 = dynamic_cast<hash_index<5>*>(idx.get());
  CHECK(ptr5 != nullptr);
}

TEST(hash index for integer) {
  factory<value_index>::initialize();
  auto t = integer_type{}.attributes({{"index", "hash"}});
  caf::settings opts;
  opts["cardinality"] = 1_Ki;
  auto idx = factory<value_index>::make(t, opts);
  REQUIRE(idx != nullptr);
  auto ptr = dynamic_cast<hash_index<3>*>(idx.get());
  REQUIRE(ptr != nullptr);
  CHECK(idx->append(make_data_view(42)));
  CHECK(idx->append(make_data_view(43)));
  CHECK(idx->append(make_data_view(44)));
  auto result = idx->lookup(not_equal, make_data_view(42));
  CHECK_EQUAL(to_string(unbox(result)), "011");
}

TEST(hash index for list) {
  factory<value_index>::initialize();
  auto t = list_type{address_type{}}.attributes({{"index", "hash"}});
  auto idx = factory<value_index>::make(t, caf::settings{});
  REQUIRE(idx != nullptr);
  auto xs = list{1, 2, 3};
  auto ys = list{7, 5, 4};
  auto zs = list{0, 0, 0};
  CHECK(idx->append(make_data_view(xs)));
  CHECK(idx->append(make_data_view(xs)));
  CHECK(idx->append(make_data_view(zs)));
  auto result = idx->lookup(equal, make_data_view(zs));
  CHECK_EQUAL(to_string(unbox(result)), "001");
  result = idx->lookup(ni, make_data_view(1));
  REQUIRE(!result);
  CHECK(result.error() == ec::unsupported_operator);
}
