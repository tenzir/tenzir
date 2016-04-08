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

TEST(construction and assignment) {
  CHECK(which(json{}) == json::type::null);
  CHECK(which(json{nil}) == json::type::null);

  CHECK(which(json{true}) == json::type::boolean);
  CHECK(which(json{false}) == json::type::boolean);

  CHECK(which(json{4.2}) == json::type::number);
  CHECK(which(json{42}) == json::type::number);
  CHECK(which(json{-1337}) == json::type::number);

  CHECK(which(json(std::string{"foo"})) == json::type::string);
  CHECK(which(json("foo")) == json::type::string);

  CHECK(which(json{json::array{}}) == json::type::array);

  CHECK(which(json{json::object{}}) == json::type::object);

  json j;
  j = nil;
  CHECK(which(j) == json::type::null);
  CHECK(is<none>(j));

  j = true;
  CHECK(which(j) == json::type::boolean);
  CHECK(is<bool>(j));

  j = 42;
  CHECK(which(j) == json::type::number);
  CHECK(is<json::number>(j));

  j = "foo";
  CHECK(which(j) == json::type::string);
  CHECK(is<std::string>(j));

  j = json::array{};
  CHECK(which(j) == json::type::array);
  CHECK(is<json::array>(j));

  j = json::object{};
  CHECK(which(j) == json::type::object);
  CHECK(is<json::object>(j));
}

TEST(total order) {
  json j0{true};
  json j1{false};

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

TEST(parsing) {
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
  str = "[ 42,-1337 , \"foo\", null ,true ]"s;
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

TEST(printing) {
  CHECK(to_string(json{}) == "null");
  CHECK(to_string(json{true}) == "true");
  CHECK(to_string(json{false}) == "false");
  CHECK(to_string(json{42}) == "42");
  CHECK(to_string(json{42.0}) == "42");
  CHECK(to_string(json{4.2}) == "4.2");
  CHECK(to_string(json{"foo"}) == "\"foo\"");

  std::string line;
  json::array a{42, -1337, "foo", nil, true};
  CHECK(printers::json<policy::oneline>(line, json{a}));
  CHECK(line == "[42, -1337, \"foo\", null, true]");

  json::object o;
  o["foo"] = 42;
  o["bar"] = nil;
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK(line == "{\"bar\": null, \"foo\": 42}");

  o = {{"baz", 4.2}};
  line.clear();
  CHECK(printers::json<policy::oneline>(line, json{o}));
  CHECK(line == "{\"baz\": 4.2}");

  o = {
    {"baz", 4.2},
    {"x", a},
    {"inner", json::object{{"a", false}, {"b", 42}, {"c", a}}}
  };

  auto json_tree = R"json({
  "baz": 4.2,
  "inner": {
    "a": false,
    "b": 42,
    "c": [
      42,
      -1337,
      "foo",
      null,
      true
    ]
  },
  "x": [
    42,
    -1337,
    "foo",
    null,
    true
  ]
})json";

  std::string str;
  CHECK(printers::json<policy::tree>(str, json{o}));
  CHECK(str == json_tree);
}

TEST(conversion) {
  auto t = to<json>(true);
  REQUIRE(t);
  CHECK(*t == json{true});

  t = to<json>(4.2);
  REQUIRE(t);
  CHECK(*t == json{4.2});

  t = to<json>("foo");
  REQUIRE(t);
  CHECK(*t == json{"foo"});

  t = to<json>(std::vector<int>{1, 2, 3});
  REQUIRE(t);
  CHECK(*t == json::array{1, 2, 3});

  t = to<json>(std::map<unsigned, bool>{{1, true}, {2, false}});
  REQUIRE(t);
  CHECK(*t == json::object{{"1", true}, {"2", false}});
}
