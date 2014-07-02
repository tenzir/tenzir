#include "vast/util/json.h"

#include "framework/unit.h"

using namespace vast;
using namespace util;

SUITE("util")

TEST("JSON construction/assignment")
{
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

TEST("JSON printing")
{
  std::string str;
  auto out = std::back_inserter(str);

  CHECK(print(json{}, out));
  CHECK(str == "null");
  str.clear();

  CHECK(print(json{true}, out));
  CHECK(str == "true");
  str.clear();

  CHECK(print(json{false}, out));
  CHECK(str == "false");
  str.clear();

  CHECK(print(json{42}, out));
  CHECK(str == "42");
  str.clear();

  CHECK(print(json{42.0}, out));
  CHECK(str == "42");
  str.clear();

  CHECK(print(json{4.2}, out));
  CHECK(str == "4.2");
  str.clear();

  CHECK(print(json{"foo"}, out));
  CHECK(str == "\"foo\"");
  str.clear();

  std::string foo{"foo"};
  CHECK(print(json{foo}, out));
  CHECK(str == "\"foo\"");
  str.clear();

  json::array a{42, -1337, "foo", nil, true};
  CHECK(print(json{a}, out));
  CHECK(str == "[42, -1337, \"foo\", null, true]");
  str.clear();

  json::object o;
  o["foo"] = 42;
  o["bar"] = nil;

  // We use a std::map, which orders the keys alphabetically.
  CHECK(print(json{std::move(o)}, out));
  CHECK(str == "{\"bar\": null, \"foo\": 42}");
  str.clear();

  o = {{"baz", 4.2}};
  CHECK(print(json{std::move(o)}, out));
  CHECK(str == "{\"baz\": 4.2}");
  str.clear();

  o = {
    {"baz", 4.2},
    {"x", a},
    {"inner", json::object{{"a", false}, {"b", 42}, {"c", a}}}
  };

  auto tree = R"json({
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

  CHECK(to_string(o, true) == tree);
  str.clear();
}
