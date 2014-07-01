#include "vast/util/json.h"

#include "framework/unit.h"

using namespace vast;
using namespace util;

SUITE("util")

TEST("JSON construction/assignment")
{
  CHECK(json{}.which() == json::type::null);
  CHECK(json{nil}.which() == json::type::null);

  CHECK(json{true}.which() == json::type::boolean);
  CHECK(json{false}.which() == json::type::boolean);

  CHECK(json{4.2}.which() == json::type::number);
  CHECK(json{42}.which() == json::type::number);
  CHECK(json{-1337}.which() == json::type::number);

  CHECK(json(std::string{"foo"}).which() == json::type::string);
  CHECK(json("foo").which() == json::type::string);

  CHECK(json{json::array{}}.which() == json::type::array);

  CHECK(json{json::object{}}.which() == json::type::object);

  json j;
  j = nil;
  CHECK(j.which() == json::type::null);

  j = true;
  CHECK(j.which() == json::type::boolean);

  j = 42;
  CHECK(j.which() == json::type::number);

  j = "foo";
  CHECK(j.which() == json::type::string);

  j = json::array{};
  CHECK(j.which() == json::type::array);

  j = json::object{};
  CHECK(j.which() == json::type::object);
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
  CHECK(print(json{std::move(a)}, out));
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
}
