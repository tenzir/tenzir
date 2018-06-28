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
using namespace std::literals;

TEST(copying views) {
  MESSAGE("calling view_t directly");
  CHECK_EQUAL(view_t<caf::none_t>{caf::none}, caf::none);
  CHECK_EQUAL(view_t<boolean>{true}, true);
  CHECK_EQUAL(view_t<integer>{42}, 42);
  CHECK_EQUAL(view_t<count>{42}, 42u);
  CHECK_EQUAL(view_t<real>{4.2}, 4.2);
  CHECK_EQUAL(view_t<port>(53, port::udp), port(53, port::udp));
  MESSAGE("using make_view");
  CHECK_EQUAL(make_view(caf::none), caf::none);
  CHECK_EQUAL(make_view(true), true);
  CHECK_EQUAL(make_view(42), integer(42));
  CHECK_EQUAL(make_view(42u), count(42u));
  CHECK_EQUAL(make_view(4.2), real(4.2));
  CHECK_EQUAL(make_view(port(53, port::udp)), port(53, port::udp));
  MESSAGE("copying from temporary data");
  CHECK_EQUAL(make_view(data{caf::none}), caf::none);
  CHECK_EQUAL(make_view(data{true}), true);
  CHECK_EQUAL(make_view(data{42}), integer(42));
  CHECK_EQUAL(make_view(data{42u}), count(42u));
  CHECK_EQUAL(make_view(data{4.2}), real(4.2));
  CHECK_EQUAL(make_view(data(port(53, port::udp))), port(53, port::udp));
}

TEST(string view) {
  std::string str = "foobar";
  auto v = make_view(str);
  CHECK_EQUAL(v, "foobar");
  str[3] = 'z';
  CHECK_EQUAL(v, "foozar");
}

TEST(vector view) {
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
  MESSAGE("check conversion back to data");
  CHECK(make_data(v) == xs);
}

TEST(set view) {
  auto xs = set{true, 42, "foo"};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  MESSAGE("check view contents");
  for (auto i = 0u; i < xs.size(); ++i)
    CHECK_EQUAL(v->at(i), make_data_view(*std::next(xs.begin(), i)));
  MESSAGE("check iterator semantics");
  CHECK_EQUAL(std::next(v->begin(), 3), v->end());
  CHECK_EQUAL(*std::next(v->begin(), 1), make_data_view(42));
  MESSAGE("check conversion back to data");
  CHECK(make_data(v) == xs);
}

TEST(map view) {
  auto xs = map{{42, true}, {84, false}};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  MESSAGE("check view contents");
  for (auto i = 0u; i < xs.size(); ++i) {
    auto [key, value] = v->at(i);
    auto& [expected_key, expected_value] = *std::next(xs.begin(), i);
    CHECK_EQUAL(key, make_data_view(expected_key));
    CHECK_EQUAL(value, make_data_view(expected_value));
  }
  MESSAGE("check iterator behavior");
  CHECK_EQUAL(std::next(v->begin(), 2), v->end());
  MESSAGE("check iterator value type");
  auto [key, value] = *v->begin();
  CHECK_EQUAL(key, make_data_view(42));
  CHECK_EQUAL(value, make_data_view(true));
  MESSAGE("check conversion back to data");
  CHECK(make_data(v) == xs);
}

TEST(make_data_view) {
  auto x = make_data_view(true);
  CHECK(caf::holds_alternative<boolean>(x));
  auto str = "foo"s;
  x = make_data_view(str);
  CHECK(caf::holds_alternative<view_t<std::string>>(x));
  CHECK(caf::holds_alternative<std::string_view>(x));
  auto xs = vector{42, true, "foo"};
  x = make_data_view(xs);
  REQUIRE(caf::holds_alternative<view_t<vector>>(x));
  auto v = caf::get<view_t<vector>>(x);
  REQUIRE_EQUAL(v->size(), 3u);
  CHECK_EQUAL(v->at(0), integer{42});
  CHECK_EQUAL(v->at(1), true);
  CHECK_EQUAL(v->at(2), "foo"sv);
}

TEST(increment decrement container_view_iterator) {
  auto xs = vector{42, true, "foo", 4.2};
  auto v = make_view(xs);
  auto it1 = v->begin();
  auto it2 = v->begin();
  CAF_CHECK_EQUAL(it1.distance_to(it2), 0u);
  ++it1;
  CAF_CHECK_NOT_EQUAL(it1.distance_to(it2), 0u);
  --it1;
  CAF_CHECK_EQUAL(it1.distance_to(it2), 0u);
}
