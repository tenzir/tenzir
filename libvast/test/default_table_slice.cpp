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

#include "vast/default_table_slice.hpp"

#define SUITE table
#include "test.hpp"

using namespace vast;

TEST(default_table_slice) {
  auto layout = record_type{
    {"a", integer_type{}},
    {"b", string_type{}},
    {"c", real_type{}}
  };
  auto builder = table_slice::builder::make<default_table_slice>(layout);
  // Or: default_table_slice::builder::make(layout)
  REQUIRE(builder);
  MESSAGE("1st row");
  CHECK(builder->add(42));
  CHECK(!builder->add(true)); // wrong type
  CHECK(builder->add("foo"));
  CHECK(builder->add(4.2));
  MESSAGE("2nd row");
  CHECK(builder->add(43));
  CHECK(builder->add("bar"));
  CHECK(builder->add(4.3));
  MESSAGE("finish");
  auto slice = builder->finish();
  CHECK_EQUAL(slice->rows(), data{2u});
  CHECK_EQUAL(slice->columns(), data{3u});
  auto x = slice->at(0, 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, data{"foo"});
  x = slice->at(1, 2);
  REQUIRE(x);
  CHECK_EQUAL(*x, data{4.3});
}
