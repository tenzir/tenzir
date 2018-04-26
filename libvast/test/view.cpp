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

#include "vast/view.hpp"

#define SUITE view
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(arithmetic view) {
  CHECK_EQUAL(view_t<boolean>{true}, true);
  CHECK_EQUAL(view_t<integer>{42}, 42);
  CHECK_EQUAL(view_t<std::string>{"foo"}, "foo");
}

TEST(make_view) {
  auto x = make_view(true);
  CHECK(std::holds_alternative<boolean>(x));
  auto str = "foo"s;
  x = make_view(str);
  CHECK(std::holds_alternative<view_t<std::string>>(x));
  CHECK(std::holds_alternative<std::string_view>(x));
  auto xs = vector{42, true, "foo"};
  x = make_view(xs);
  REQUIRE(std::holds_alternative<view_t<vector>>(x));
  auto v = get<view_t<vector>>(x);
  REQUIRE_EQUAL(v->size(), 3u);
  CHECK_EQUAL(v->at(0), 42);
  CHECK_EQUAL(v->at(1), true);
  CHECK_EQUAL(v->at(2), "foo");
}
