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

#define SUITE json

#include "vast/test/test.hpp"

#include "vast/json.hpp"

#include <chrono>

#include "vast/concept/parseable/vast/json.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/time.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(construction) {
  CHECK(caf::holds_alternative<json::null>(json{}));
  CHECK(caf::holds_alternative<json::null>(json{caf::none}));
  CHECK(caf::holds_alternative<json::boolean>(json{true}));
  CHECK(caf::holds_alternative<json::number>(json{42}));
  CHECK(caf::holds_alternative<json::number>(json{4.2}));
  CHECK(caf::holds_alternative<json::string>(json{"foo"}));
  CHECK(caf::holds_alternative<json::string>(json{"foo"s}));
  CHECK(caf::holds_alternative<json::array>(json{json::make_array(1, 2, 3)}));
  CHECK(caf::holds_alternative<json::object>(json{json::object{{"foo", json{42}}}}));
}

TEST(assignment) {
  json j;
  j = caf::none;
  CHECK(caf::holds_alternative<json::null>(j));
  j = true;
  CHECK(caf::holds_alternative<json::boolean>(j));
  j = 42;
  CHECK(caf::holds_alternative<json::number>(j));
  j = "foo";
  CHECK(caf::holds_alternative<std::string>(j));
  j = json::make_array(true, false);
  CHECK(caf::holds_alternative<json::array>(j));
  j = json::object{{"x", json{true}}, {"y", json{false}}};
  CHECK(caf::holds_alternative<json::object>(j));
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
  CHECK(j == caf::none);
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
  CHECK(j == json::make_array(42, -1337, "foo", caf::none, true));
  MESSAGE("object");
  str = "{}";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{});
  str = "{    }";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{});
  str = R"json({ "baz": 4.2, "inner": null })json";
  CHECK(parsers::json(str, j));
  CHECK(j == json::object{{"baz", json{4.2}}, {"inner", json{caf::none}}});
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
    {"baz", json{4.2}},
    {"inner", json{caf::none}},
    {"x", json{json::make_array(42, -1337, "foo", true)}}
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
  auto a = json::make_array(42, -1337, "foo", caf::none, true);
  CHECK(printers::json<policy::oneline>(line, json{a}));
  CHECK_EQUAL(line, "[42, -1337, \"foo\", null, true]");
  json::object o;
  o["foo"] = 42;
  o["bar"] = caf::none;
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK_EQUAL(line, "{\"foo\": 42, \"bar\": null}");
  o = {{"baz", json{4.2}}};
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK_EQUAL(line, "{\"baz\": 4.2}");
  MESSAGE("tree policy");
  o = {
    {"baz", json{4.2}},
    {"x", json{a}},
    {"inner", json{json::object{
     {"a", json{false}},
     {"c", json{a}},
     {"b", json{42}}}}}};
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
  using namespace std::chrono;
  auto since_epoch = timespan{1258532203657267968ll};
  auto ts = timestamp{since_epoch};
  auto fractional_ts = duration_cast<double_seconds>(since_epoch).count();
  CHECK_EQUAL(to_json(true), json{true});
  CHECK_EQUAL(to_json(4.2), json{4.2});
  CHECK_EQUAL(to_json(since_epoch), json{fractional_ts});
  CHECK_EQUAL(to_json(ts), json{fractional_ts});
  CHECK_EQUAL(to_json("foo"), json{"foo"});
  MESSAGE("std::vector");
  auto xs = to_json(std::vector<int>{1, 2, 3});
  CHECK_EQUAL(xs, json::make_array(1, 2, 3));
  MESSAGE("std::map");
  auto ys = to_json(std::map<unsigned, bool>{{1, true}, {2, false}});
  CHECK_EQUAL(ys, (json::object{{"1", json{true}}, {"2", json{false}}}));
}
