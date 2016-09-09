#include "vast/data.hpp"
#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/type.hpp"
#include "vast/save.hpp"
#include "vast/concept/convertible/vast/type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/type.hpp"

#define SUITE type
#include "test.hpp"

using namespace vast;

TEST(printing) {
  CHECK(to_string(type{}) == "none");
  CHECK(to_string(type::boolean{}) == "bool");
  CHECK(to_string(type::integer{}) == "int");
  CHECK(to_string(type::count{}) == "count");
  CHECK(to_string(type::real{}) == "real");
  CHECK(to_string(type::time_point{}) == "time");
  CHECK(to_string(type::time_duration{}) == "duration");
  CHECK(to_string(type::string{}) == "string");
  CHECK(to_string(type::pattern{}) == "pattern");
  CHECK(to_string(type::address{}) == "addr");
  CHECK(to_string(type::subnet{}) == "subnet");
  CHECK(to_string(type::port{}) == "port");

  std::vector<std::string> fields = {"foo", "bar", "baz"};
  auto e = type::enumeration{std::move(fields)};
  CHECK(to_string(e) == "enum {foo, bar, baz}");

  type t = type::vector{type::real{}};
  CHECK(to_string(t) == "vector<real>");

  t = type::set{type::port{}, {type::attribute::skip}};
  CHECK(to_string(t) == "set<port> &skip");

  t = type::table{type::count{}, t};
  CHECK(to_string(t) == "table<count, set<port> &skip>");

  auto r = type::record{{
        {"foo", t},
        {"bar", type::integer{}},
        {"baz", type::real{}}
      }};

  CHECK(to_string(r)
        == "record {foo: table<count, set<port> &skip>, bar: int, baz: real}");

  type a = type::alias{t};
  CHECK(to_string(a) == to_string(t));
  a.name("qux");

  std::string sig;
  CHECK(printers::type<policy::signature>(sig, a));
  CHECK(sig == "qux = table<count, set<port> &skip>");
}

TEST(equality comparison) {
  type t = type::boolean{};
  type u = type::boolean{};
  CHECK(t == u);

  // The name is part of the type signature.
  CHECK(t.name("foo"));
  CHECK(t != u);
  CHECK(u.name("foo"));
  CHECK(t == u);

  // Names can only be assigned once.
  CHECK(!t.name("bar"));
  CHECK(t == u);

  // But we can always create a new type instance...
  t = type::boolean{};
  CHECK(t.name("foo"));
  CHECK(t == u);

  // ...as long as it has the same type signature.
  t = type::count{};
  CHECK(t.name("foo"));
  CHECK(t != u);
}

TEST(serialization) {
  type s0 = type::string{{type::attribute::skip}};
  type t = type::set{type::port{}};
  t = type::table{type::count{}, t, {type::attribute::skip}};

  std::vector<char> buf;
  save(buf, s0, t);

  type u, s1;
  load(buf, s1, u);
  CHECK(s0 == s1);
  CHECK(to_string(s1) == "string &skip");
  CHECK(u == t);
  CHECK(to_string(t) == "table<count, set<port>> &skip");
}

TEST(record range) {
  auto r = type::record{
    {"x", type::record{
            {"y", type::record{
                    {"z", type::integer{}},
                    {"k", type::boolean{}}
                  }},
            {"m", type::record{
                    {"y", type::record{{"a", type::address{}}}},
                    {"f", type::real{}}
                  }},
            {"b", type::boolean{}}
          }},
    {"y", type::record{{"b", type::boolean{}}}}
  };

  for (auto& i : type::record::each{r})
    if (i.offset == offset{0, 1, 0, 0})
      CHECK(i.key() == key{"x", "m", "y", "a"});
    else if (i.offset == offset{1, 0})
      CHECK(i.key() == key{"y", "b"});
}

TEST(record resolving) {
  auto r = type::record{
    {"x", type::integer{}},
    {"y", type::address{}},
    {"z", type::real{}}
  };

  r = {
    {"a", type::integer{}},
    {"b", type::count{}},
    {"c", r}
  };

  auto o = r.resolve(key{"c"});
  REQUIRE(o);
  CHECK(o->size() == 1);
  CHECK(o->front() == 2);

  o = r.resolve(key{"c", "x"});
  REQUIRE(o);
  CHECK(o->size() == 2);
  CHECK(o->front() == 2);
  CHECK(o->back() == 0);

  auto k = r.resolve(offset{2});
  REQUIRE(k);
  CHECK(k->size() == 1);
  CHECK(k->front() == "c");

  k = r.resolve(offset{2, 0});
  REQUIRE(k);
  CHECK(k->size() == 2);
  CHECK(k->front() == "c");
  CHECK(k->back() == "x");
}

TEST(record flattening/unflattening) {
  auto x = type::record{
    {"x", type::record{
            {"y", type::record{
                    {"z", type::integer{}},
                    {"k", type::boolean{}}
                  }},
            {"m", type::record{
                    {"y", type::record{{"a", type::address{}}}},
                    {"f", type::real{}}
                  }},
            {"b", type::boolean{}}
          }},
    {"y", type::record{{"b", type::boolean{}}}}
  };
  auto y = type::record{{
    {"x.y.z", type::integer{}},
    {"x.y.k", type::boolean{}},
    {"x.m.y.a", type::address{}},
    {"x.m.f", type::real{}},
    {"x.b", type::boolean{}},
    {"y.b", type::boolean{}}
  }};
  auto f = flatten(x);
  CHECK(f == y);
  auto u = unflatten(f);
  CHECK(u == x);
}

TEST(record symbol finding) {
  auto r = type::record{
    {"x", type::integer{}},
    {"y", type::address{}},
    {"z", type::real{}}
  };
  r = {
    {"a", type::integer{}},
    {"b", type::count{}},
    {"c", type::record{r}}
  };
  r = {
    {"a", type::integer{}},
    {"b", type::record{r}},
    {"c", type::count{}}
  };
  r.name("foo");
  // Record access by key.
  auto first = r.at(key{"a"});
  REQUIRE(first);
  CHECK(is<type::integer>(*first));
  auto deep = r.at(key{"b", "c", "y"});
  REQUIRE(deep);
  CHECK(is<type::address>(*deep));
  //
  // Prefix finding.
  //
  auto o = r.find_prefix({"a"});
  CHECK(o.size() == 0);
  o = r.find_prefix({"foo", "a"});
  offset a{0};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == a);
  o = r.find_prefix({"foo", "b", "a"});
  offset ba{1, 0};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == ba);
  //
  // Suffix finding.
  //
  o = r.find_suffix({"z"});
  offset z{1, 2, 2};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == z);
  o = r.find_suffix({"c", "y"});
  offset cy{1, 2, 1};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == cy);
  o = r.find_suffix({"a"});
  offset a0{0}, a1{1, 0};
  REQUIRE(o.size() == 2);
  CHECK(o[0].first == a0);
  CHECK(o[1].first == a1);
  o = r.find_suffix({"c", "*"});
  offset c0{1, 2, 0}, c1{1, 2, 1}, c2{1, 2, 2};
  REQUIRE(o.size() == 3);
  CHECK(o[0].first == c0);
  CHECK(o[1].first == c1);
  CHECK(o[2].first == c2);
}

TEST(congruence) {
  MESSAGE("basic");
  auto i = type::integer{};
  i.name("i");
  auto j = type::integer{};
  j.name("j");
  auto c = type::count{};
  c.name("c");
  CHECK(congruent(i, i));
  CHECK(congruent(i, j));
  CHECK(!congruent(i, c));
  MESSAGE("sets");
  auto s0 = type::set{i};
  auto s1 = type::set{j};
  auto s2 = type::set{c};
  CHECK(s0 != s1);
  CHECK(congruent(s0, s1));
  CHECK(! congruent(s1, s2));
  MESSAGE("records");
  auto r0 = type::record{
    {"a", type::address{}},
    {"b", type::boolean{}},
    {"c", type::count{}}};
  auto r1 = type::record{
    {"x", type::address{}},
    {"y", type::boolean{}},
    {"z", type::count{}}};
  CHECK(r0 != r1);
  CHECK(congruent(r0, r1));
  MESSAGE("aliases");
  type a = type::alias{i};
  a.name("a");
  CHECK(a != i);
  CHECK(congruent(a, i));
  a = type::alias{r0};
  a.name("r0");
  CHECK(a != r0);
  CHECK(congruent(a, r0));
}

TEST(derivation) {
  CHECK_EQUAL(type::derive(data{"foo"}), type::string{});
  type::record r{
    {"", type::integer{}},
    {"", type::count{}},
    {"", type::real{}}
  };
  CHECK_EQUAL(type::derive(record{42, 1337u, 3.1415}), r);
}

TEST(attributes) {
  // Attributes are key-value pairs...
  type::vector v{type::integer{}, {type::attribute::skip}};
  auto a = v.find_attribute(type::attribute::skip);
  REQUIRE(a);
  CHECK(a->value == "");
  // Attributes are part of the type signature.
  CHECK(v != type::vector{type::integer{}});
}

TEST(json conversion) {
  auto e = type::enumeration{{"foo", "bar", "baz"}};
  e.name("e");
  auto t = type::table{type::boolean{}, type::count{}};
  t.name("bit_table");
  auto r = type::record{
    {"x", type::address{{type::attribute::skip}}},
    {"y", type::boolean{{{type::attribute::default_, "F"}}}},
    {"z", type::record{{"inner", e}}}
  };
  r.name("foo");
  auto expected = R"__({
  "attributes": [],
  "kind": "record",
  "name": "foo",
  "structure": {
    "x": {
      "attributes": [
        "skip"
      ],
      "kind": "address",
      "name": "",
      "structure": null
    },
    "y": {
      "attributes": [
        [
          "default",
          "F"
        ]
      ],
      "kind": "boolean",
      "name": "",
      "structure": null
    },
    "z": {
      "attributes": [],
      "kind": "record",
      "name": "",
      "structure": {
        "inner": {
          "attributes": [],
          "kind": "enumeration",
          "name": "e",
          "structure": [
            "foo",
            "bar",
            "baz"
          ]
        }
      }
    }
  }
})__";
  CHECK(to_string(to_json(r)) == expected);
}
