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
  auto builder = default_table_slice::builder::make(layout);
  REQUIRE(builder);
  MESSAGE("1st row");
  CHECK(builder->add(make_view(42)));
  CHECK(!builder->add(make_view(true))); // wrong type
  CHECK(builder->add(make_view("foo")));
  CHECK(builder->add(make_view(4.2)));
  MESSAGE("2nd row");
  CHECK(builder->append(43));
  CHECK(builder->append("bar"));
  CHECK(builder->append(4.3));
  MESSAGE("finish");
  auto slice = builder->finish();
  CHECK_EQUAL(slice->rows(), 2u);
  CHECK_EQUAL(slice->columns(), 3u);
  auto x = slice->at(0, 1);
  REQUIRE(x);
  CHECK_EQUAL(*x, data{"foo"});
  x = slice->at(1, 2);
  REQUIRE(x);
  CHECK_EQUAL(*x, data_view{4.3});
}
