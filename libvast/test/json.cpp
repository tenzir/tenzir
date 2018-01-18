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

#include "vast/json.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/convertible/to.hpp"

#define SUITE json
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(construction) {
  CHECK(is<json::null>(json{}));
  CHECK(is<json::null>(json{nil}));
  CHECK(is<json::boolean>(json{true}));
  CHECK(is<json::boolean>(json{false}));
  CHECK(is<json::number>(json{4.2}));
  CHECK(is<json::number>(json{42u}));
  CHECK(is<json::number>(json{-1337}));
  CHECK(is<json::string>(json{"foo"}));
  CHECK(is<json::string>(json{"foo"s}));
  CHECK(is<json::array>(json{json::array{{1, 2, 3}}}));
  CHECK(is<json::object>(json{json::object{{"foo", 42}}}));
}

TEST(assignment) {
  json j;
  j = nil;
  CHECK(is<json::null>(j));
  j = true;
  CHECK(is<json::boolean>(j));
  j = 42;
  CHECK(is<json::number>(j));
  j = "foo";
  CHECK(is<std::string>(j));
  j = json::array{true, false};
  CHECK(is<json::array>(j));
  j = json::object{{"x", true}, {"y", false}};
  CHECK(is<json::object>(j));
}

TEST(total order) {
  auto j0 = json{true};
  auto j1 = json{false};
  CHECK(j1 < j0);
  CHECK(j0 != j1);
  j0 = "bar";
  j1 = "foo";
  CHECK(j0 != j1);
  CHECK(j0 < j1);
  j1 = 42;
  CHECK(j0 != j1);
  CHECK(!(j0 < j1));
  CHECK(!(j0 <= j1));
  CHECK(j0 > j1);
  CHECK(j0 >= j1);
}

TEST(parseable) {
  json j;
  std::string str;
  MESSAGE("bool");
  str = "true";
  CHECK(parsers::json(str, j));
  CHECK(j == true);
  str = "false";
  CHECK(parsers::json(str, j));
  CHECK(j == false);
  MESSAGE("null");
  str = "null";
  CHECK(parsers::json(str, j));
  CHECK(j == nil);
  MESSAGE("number");
  str = "42";
  CHECK(parsers::json(str, j));
  CHECK(j == json::number{42});
  str = "-1337";
  CHECK(parsers::json(str, j));
  CHECK(j == json::number{-1337});
  str = "4.2";
  CHECK(parsers::json(str, j));
  CHECK(j == json::number{4.2});
  MESSAGE("string");
  str = "\"foo\"";
  CHECK(parsers::json(str, j));
  CHECK(j == "foo");
  MESSAGE("array");
  str = "[]";
  CHECK(parsers::json(str, j));
  CHECK(j == json::array{});
  str = "[    ]";
  CHECK(parsers::json(str, j));
  CHECK(j == json::array{});
  str = R"([ 42,-1337 , "foo", null ,true ])";
  CHECK(parsers::json(str, j));
  CHECK(j == json::array{42, -1337, "foo", nil, true});
  MESSAGE("object");
  str = "{}";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{});
  str = "{    }";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{});
  str = R"json({ "baz": 4.2, "inner": null })json";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{{"baz", 4.2}, {"inner", nil}});
  str = R"json({
  "baz": 4.2,
  "inner": null,
  "x": [
    42,
    -1337,
    "foo",
    true
  ]
})json";
  CHECK(parsers::json(str, j));
  auto o = json::object{
    {"baz", 4.2},
    {"inner", nil},
    {"x", json::array{42, -1337, "foo", true}}
  };
  CHECK(j == o);
}

TEST(printable) {
  CHECK_EQUAL(to_string(json{}), "null");
  CHECK_EQUAL(to_string(json{true}), "true");
  CHECK_EQUAL(to_string(json{false}), "false");
  CHECK_EQUAL(to_string(json{42}), "42");
  CHECK_EQUAL(to_string(json{42.0}), "42");
  CHECK_EQUAL(to_string(json{4.2}), "4.2");
  CHECK_EQUAL(to_string(json{"foo"}), "\"foo\"");
  MESSAGE("one line policy");
  std::string line;
  json::array a{42, -1337, "foo", nil, true};
  CHECK(printers::json<policy::oneline>(line, json{a}));
  CHECK_EQUAL(line, "[42, -1337, \"foo\", null, true]");
  json::object o;
  o["foo"] = 42;
  o["bar"] = nil;
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK_EQUAL(line, "{\"foo\": 42, \"bar\": null}");
  o = {{"baz", 4.2}};
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK_EQUAL(line, "{\"baz\": 4.2}");
  MESSAGE("tree policy");
  o = {
    {"baz", 4.2},
    {"x", a},
    {"inner", json::object{{"a", false}, {"c", a}, {"b", 42}}}
  };
  auto json_tree = R"json({
  "baz": 4.2,
  "x": [
    42,
    -1337,
    "foo",
    null,
    true
  ],
  "inner": {
    "a": false,
    "c": [
      42,
      -1337,
      "foo",
      null,
      true
    ],
    "b": 42
  }
})json";
  std::string str;
  CHECK(printers::json<policy::tree>(str, json{o}));
  CHECK_EQUAL(str, json_tree);
}

TEST(conversion) {
  MESSAGE("bool");
  auto t = to<json>(true);
  REQUIRE(t);
  CHECK(*t == json{true});
  MESSAGE("number");
  t = to<json>(4.2);
  REQUIRE(t);
  CHECK(*t == json{4.2});
  MESSAGE("strings");
  t = to<json>("foo");
  REQUIRE(t);
  CHECK(*t == json{"foo"});
  MESSAGE("std::vector");
  t = to<json>(std::vector<int>{1, 2, 3});
  REQUIRE(t);
  CHECK(*t == json::array{1, 2, 3});
  MESSAGE("std::map");
  t = to<json>(std::map<unsigned, bool>{{1, true}, {2, false}});
  REQUIRE(t);
  CHECK(*t == json::object{{"1", true}, {"2", false}});
}
