//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/index/list_index.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/test/test.hpp"
#include "vast/value_index_factory.hpp"

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

TEST(list) {
  auto container_type = type{list_type{string_type{}}};
  list_index idx{container_type};
  MESSAGE("append");
  list xs{"foo", "bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"qux", "foo", "baz", "corge"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs), 7));
  MESSAGE("lookup");
  auto x = "foo"s;
  CHECK_EQUAL(
    to_string(*idx.lookup(relational_operator::ni, make_data_view(x))), "110000"
                                                                        "00");
  CHECK_EQUAL(
    to_string(*idx.lookup(relational_operator::not_ni, make_data_view(x))),
    "00110001");
  x = "bar";
  CHECK_EQUAL(
    to_string(*idx.lookup(relational_operator::ni, make_data_view(x))), "101100"
                                                                        "01");
  x = "not";
  CHECK_EQUAL(
    to_string(*idx.lookup(relational_operator::ni, make_data_view(x))), "000000"
                                                                        "00");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  list_index idx2{container_type};
  CHECK_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  x = "foo";
  CHECK_EQUAL(
    to_string(*idx2.lookup(relational_operator::ni, make_data_view(x))), "11000"
                                                                         "000");
}

FIXTURE_SCOPE_END()
