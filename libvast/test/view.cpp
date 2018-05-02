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
  CHECK_EQUAL(view_t<port>(53, port::udp), port(53, port::udp));
}

TEST(container view) {
  auto xs = vector{42, true, "foo", 4.2};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  auto i = v->begin();
  CHECK_EQUAL(*i, v->at(0));
  CHECK_EQUAL(*i, make_data_view(42));
  ++i;
  CHECK_EQUAL(*i, v->at(1));
  CHECK_EQUAL(*i, make_data_view(true));
  i += 2;
  CHECK_EQUAL(*i, v->at(3));
  CHECK_EQUAL(*i, make_data_view(4.2));
  ++i;
  CHECK_EQUAL(i, v->end());
  auto j = v->begin() + 1;
  CHECK_EQUAL(i - j, xs.size() - 1);
}

TEST(make_data_view) {
  auto x = make_data_view(true);
  CHECK(std::holds_alternative<boolean>(x));
  auto str = "foo"s;
  x = make_data_view(str);
  CHECK(std::holds_alternative<view_t<std::string>>(x));
  CHECK(std::holds_alternative<std::string_view>(x));
  auto xs = vector{42, true, "foo"};
  x = make_data_view(xs);
  REQUIRE(std::holds_alternative<view_t<vector>>(x));
  auto v = get<view_t<vector>>(x);
  REQUIRE_EQUAL(v->size(), 3u);
  CHECK_EQUAL(v->at(0), 42);
  CHECK_EQUAL(v->at(1), true);
  CHECK_EQUAL(v->at(2), "foo");
}
