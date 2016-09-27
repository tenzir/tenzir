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
  auto v = std::vector<data>{-42, 42, 84};
  CHECK(v[0] == -42);
  auto u = vector{std::move(v)};
  CHECK(u[0] == -42);
}

TEST(set) {
  auto s = std::set<data>{1, 2, 3};
  REQUIRE_EQUAL(s.size(), 3u);
  CHECK_EQUAL(*s.begin(), 1);
  CHECK_EQUAL(*s.rbegin(), 3);
}

TEST(tables) {
  table ports{{"ssh", 22u}, {"http", 80u}, {"https", 443u}, {"imaps", 993u}};
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

TEST(records) {
  MESSAGE("construction");
  auto v = vector{"foo", -42, 1001u, "x", port{443, port::tcp}};
  vector w{100, "bar", v};
  CHECK(v.size() == 5);
  MESSAGE("access via offset");
  CHECK(*at(offset{0}, w) == 100);
  CHECK(*at(offset{1}, w) == "bar");
  CHECK(*at(offset{2}, w) == v);
  CHECK(*at(offset{2, 3}, w) == data{"x"});
  MESSAGE("flatten");
  auto structured =
    vector{"foo", vector{-42, vector{1001u}}, "x", port{443, port::tcp}};
  auto flat = vector{"foo", -42, 1001u, "x", port{443, port::tcp}};
  auto flattened = flatten(structured);
  CHECK(flattened == flat);
  MESSAGE("unflatten");
  auto t = record_type{
    {"foo", string_type{}},
    {"r0", record_type{{
      {"i", integer_type{}},
      {"r1", record_type{{
        {"c", count_type{}}}}}}}},
    {"bar", string_type{}},
    {"baz", port_type{}}
  };
  auto attempt = unflatten(v, t);
  REQUIRE(attempt);
  CHECK(*attempt == structured);
  MESSAGE("serialization");
  std::string buf;
  save(buf, structured);
  decltype(structured) structured2;
  load(buf, structured2);
  CHECK(structured2 == structured);
}

TEST(construction) {
  CHECK(is<none>(data{}));
  CHECK(is<boolean>(data{true}));
  CHECK(is<boolean>(data{false}));
  CHECK(is<integer>(data{0}));
  CHECK(is<integer>(data{42}));
  CHECK(is<integer>(data{-42}));
  CHECK(is<count>(data{42u}));
  CHECK(is<real>(data{4.2}));
  CHECK(is<std::string>(data{"foo"}));
  CHECK(is<std::string>(data{std::string{"foo"}}));
  CHECK(is<pattern>(data{pattern{"foo"}}));
  CHECK(is<address>(data{address{}}));
  CHECK(is<subnet>(data{subnet{}}));
  CHECK(is<port>(data{port{53, port::udp}}));
  CHECK(is<vector>(data{vector{}}));
  CHECK(is<set>(data{set{}}));
  CHECK(is<table>(data{table{}}));
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
  d2 = nil;
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

TEST(evaluation) {
  data lhs{"foo"};
  data rhs{"foobar"};
  CHECK(evaluate(lhs, in, rhs));
  CHECK(evaluate(rhs, not_in, lhs));
  CHECK(evaluate(rhs, ni, lhs));
  CHECK(evaluate(rhs, not_in, lhs));

  lhs = count{42};
  rhs = count{1337};
  CHECK(evaluate(lhs, less_equal, rhs));
  CHECK(evaluate(lhs, less, rhs));
  CHECK(evaluate(lhs, not_equal, rhs));
  CHECK(!evaluate(lhs, equal, rhs));

  lhs = *to<address>("10.0.0.1");
  rhs = *to<subnet>("10.0.0.0/8");
  CHECK(evaluate(lhs, in, rhs));

  rhs = real{4.2};
  CHECK(!evaluate(lhs, equal, rhs));
  CHECK(evaluate(lhs, not_equal, rhs));
}

TEST(serialization) {
  set s;
  s.emplace(port{80, port::tcp});
  s.emplace(port{53, port::udp});
  s.emplace(port{8, port::icmp});

  data d0{s};
  data d1;
  std::vector<char> buf;
  save(buf, d0);
  load(buf, d1);
  CHECK(d0 == d1);
  CHECK(to_string(d1) == "{8/icmp, 53/udp, 80/tcp}");
}

TEST(parseable) {
  auto p = make_parser<data>();
  data d;

  MESSAGE("bool");
  auto str = "T"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == true);

  MESSAGE("numbers");
  str = "+1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001);
  str = "1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001u);
  str = "10.01"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == 10.01);

  MESSAGE("string");
  str = "\"bar\""s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == "bar");

  MESSAGE("pattern");
  str = "/foo/"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == pattern{"foo"});

  MESSAGE("address");
  str = "10.0.0.1"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == *to<address>("10.0.0.1"));

  MESSAGE("port");
  str = "22/tcp"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == port{22, port::tcp});

  MESSAGE("vector");
  str = "[42,4.2,nil]"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == vector{42u, 4.2, nil});

  MESSAGE("set");
  str = "{-42,+42,-1}"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == set{-42, -1, 42});

  MESSAGE("table");
  str = "{T->1,F->0}"s;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, d));
  CHECK(f == l);
  CHECK(d == table{{true, 1u}, {false, 0u}});
}

TEST(json) {
  data r = vector{"foo", vector{-42, vector{1001u}}, "x", port{443, port::tcp}};
  MESSAGE("plain");
  auto expected = R"__([
  "foo",
  [
    -42,
    [
      1001
    ]
  ],
  "x",
  "443/tcp"
])__";
  CHECK(to_json(r), expected);
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
  CHECK(type_check(t, r));
  expected = R"__({
  "port": "443/tcp",
  "r": {
    "i": -42,
    "r": {
      "u": 1001
    }
  },
  "str": "x",
  "x": "foo"
})__";
  CHECK(to_string(to_json(r, t)) == expected);
}
