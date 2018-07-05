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

#include "vast/data.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/json.hpp"

#define SUITE data
#include "test.hpp"

using namespace vast;

TEST(vector) {
  REQUIRE(std::is_same_v<std::vector<data>, vector>);
}

TEST(set) {
  auto xs = set{1, 2, 3};
  REQUIRE_EQUAL(xs.size(), 3u);
  CHECK_EQUAL(*xs.begin(), data{1});
  CHECK_EQUAL(*xs.rbegin(), data{3});
}

TEST(tables) {
  map ports{{"ssh", 22u}, {"http", 80u}, {"https", 443u}, {"imaps", 993u}};
  CHECK(ports.size() == 4);
  auto i = ports.find("ssh");
  REQUIRE(i != ports.end());
  CHECK(i->second == 22u);
  i = ports.find("imaps");
  REQUIRE(i != ports.end());
  CHECK(i->second == 993u);
  CHECK(ports.emplace("telnet", 23u).second);
  CHECK(!ports.emplace("http", 8080u).second);
}

TEST(get) {
  auto v = vector{"foo", -42, 1001u, "x", port{443, port::tcp}};
  vector w{100, "bar", v};
  CHECK(v.size() == 5);
  MESSAGE("access via offset");
  CHECK(*get(w, offset{0}) == 100);
  CHECK(*get(w, offset{1}) == "bar");
  CHECK(*get(w, offset{2}) == v);
  CHECK(*get(w, offset{2, 3}) == data{"x"});
  CHECK(*get(data{w}, offset{2, 2}) == data{1001u});
}

TEST(flatten) {
  MESSAGE("flatten");
  auto t = record_type{
    {"a", string_type{}},
    {"b", record_type{
      {"c", integer_type{}},
      {"d", vector_type{integer_type{}}}
    }},
    {"e", record_type{
      {"f", address_type{}},
      {"g", port_type{}}
    }},
    {"f", boolean_type{}}
  };
  auto xs = vector{"foo", vector{-42, vector{1, 2, 3}}, caf::none, true};
  auto ys = vector{"foo", -42, vector{1, 2, 3}, caf::none, caf::none, true};
  auto zs = flatten(xs, t);
  REQUIRE(zs);
  CHECK_EQUAL(*zs, ys);
  MESSAGE("unflatten");
  zs = unflatten(ys, t);
  REQUIRE(zs);
  CHECK_EQUAL(*zs, xs);
}

TEST(construction) {
  CHECK(caf::holds_alternative<caf::none_t>(data{}));
  CHECK(caf::holds_alternative<boolean>(data{true}));
  CHECK(caf::holds_alternative<boolean>(data{false}));
  CHECK(caf::holds_alternative<integer>(data{0}));
  CHECK(caf::holds_alternative<integer>(data{42}));
  CHECK(caf::holds_alternative<integer>(data{-42}));
  CHECK(caf::holds_alternative<count>(data{42u}));
  CHECK(caf::holds_alternative<real>(data{4.2}));
  CHECK(caf::holds_alternative<std::string>(data{"foo"}));
  CHECK(caf::holds_alternative<std::string>(data{std::string{"foo"}}));
  CHECK(caf::holds_alternative<pattern>(data{pattern{"foo"}}));
  CHECK(caf::holds_alternative<address>(data{address{}}));
  CHECK(caf::holds_alternative<subnet>(data{subnet{}}));
  CHECK(caf::holds_alternative<port>(data{port{53, port::udp}}));
  CHECK(caf::holds_alternative<vector>(data{vector{}}));
  CHECK(caf::holds_alternative<set>(data{set{}}));
  CHECK(caf::holds_alternative<map>(data{map{}}));
}

TEST(relational_operators) {
  data d1;
  data d2;
  CHECK(d1 == d2);
  CHECK(!(d1 < d2));
  CHECK(d1 <= d2);
  CHECK(d1 >= d2);
  CHECK(!(d1 > d2));

  d2 = 42;
  CHECK(d1 != d2);
  CHECK(d1 < d2);
  CHECK(d1 <= d2);
  CHECK(!(d1 >= d2));
  CHECK(!(d1 > d2));

  d1 = 42;
  d2 = caf::none;
  CHECK(d1 != d2);
  CHECK(!(d1 < d2));
  CHECK(!(d1 <= d2));
  CHECK(d1 >= d2);
  CHECK(d1 > d2);

  d2 = 1377;
  CHECK(d1 != d2);
  CHECK(d1 < d2);
  CHECK(d1 <= d2);
  CHECK(!(d1 >= d2));
  CHECK(!(d1 > d2));
}

TEST(addtion) {
  auto x = data{42};
  auto y = data{1};
  CHECK_EQUAL(x + y, data{43});
  y = caf::none;
  CHECK_EQUAL(x + y, x);
  y = vector{"foo", 3.14};
  CHECK_EQUAL(x + y, (vector{42, "foo", 3.14}));
  CHECK_EQUAL(y + x, (vector{"foo", 3.14, 42}));
}

TEST(evaluation) {
  MESSAGE("in");
  data lhs{"foo"};
  data rhs{"foobar"};
  CHECK(evaluate(lhs, in, rhs));
  CHECK(evaluate(rhs, not_in, lhs));
  CHECK(evaluate(rhs, ni, lhs));
  CHECK(evaluate(rhs, not_in, lhs));
  MESSAGE("equality");
  lhs = count{42};
  rhs = count{1337};
  CHECK(evaluate(lhs, less_equal, rhs));
  CHECK(evaluate(lhs, less, rhs));
  CHECK(evaluate(lhs, not_equal, rhs));
  CHECK(!evaluate(lhs, equal, rhs));
  MESSAGE("network types");
  lhs = *to<address>("10.0.0.1");
  rhs = *to<subnet>("10.0.0.0/8");
  CHECK(evaluate(lhs, in, rhs));
  lhs = *to<subnet>("10.0.42.0/16");
  CHECK(evaluate(lhs, in, rhs));
  rhs = *to<subnet>("10.0.42.0/17");
  CHECK(!evaluate(lhs, in, rhs));
  MESSAGE("mixed types");
  rhs = real{4.2};
  CHECK(!evaluate(lhs, equal, rhs));
  CHECK(evaluate(lhs, not_equal, rhs));
}

TEST(serialization) {
  set xs;
  xs.emplace(port{80, port::tcp});
  xs.emplace(port{53, port::udp});
  xs.emplace(port{8, port::icmp});
  auto x0 = data{xs};
  std::vector<char> buf;
  save(buf, x0);
  data x1;
  load(buf, x1);
  CHECK(x0 == x1);
}

TEST(printable) {
  // Ensure that we don't produce trailing zeros for floating point data.
  auto x = data{-4.2};
  CHECK_EQUAL(to_string(x), "-4.2");
  x = 3.14;
  CHECK_EQUAL(to_string(x), "3.14");
}

TEST(parseable) {
  auto p = make_parser<data>();
  data d;
  MESSAGE("bool");
  auto str = "T"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == true);
  MESSAGE("numbers");
  str = "+1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001);
  str = "1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001u);
  str = "10.01"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 10.01);
  MESSAGE("string");
  str = R"("bar")";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == "bar");
  MESSAGE("pattern");
  str = "/foo/"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == pattern{"foo"});
  MESSAGE("address");
  str = "10.0.0.1"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == *to<address>("10.0.0.1"));
  MESSAGE("port");
  str = "22/tcp"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == port{22, port::tcp});
  MESSAGE("vector");
  str = "[42,4.2,nil]"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == vector{42u, 4.2, caf::none});
  MESSAGE("set");
  str = "{-42,+42,-1}"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == set{-42, 42, -1});
  MESSAGE("map");
  str = "{T->1,F->0}"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == map{{true, 1u}, {false, 0u}});
}

TEST(json) {
  MESSAGE("plain");
  data x = vector{"foo", vector{-42, vector{1001u}}, "x", port{443, port::tcp}};
  json expected = json{json::make_array(
    "foo",
    json::make_array(-42, json::make_array(1001)),
    "x",
    "443/tcp"
  )};
  CHECK_EQUAL(to_json(x), expected);
  MESSAGE("zipped");
  type t = record_type{
    {"x", string_type{}},
    {"r", record_type{
      {"i", integer_type{}},
      {"r", record_type{
        {"u", count_type{}}
      }},
    }},
    {"str", string_type{}},
    {"port", port_type{}}
  };
  CHECK(type_check(t, x));
  expected = R"__({
  "x": "foo",
  "r": {
    "i": -42,
    "r": {
      "u": 1001
    }
  },
  "str": "x",
  "port": "443/tcp"
})__";
  CHECK_EQUAL(to_string(to_json(x, t)), expected);
}
