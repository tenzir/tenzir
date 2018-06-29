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
#include "vast/type.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/type.hpp"

#define SUITE type
#include "test.hpp"

using caf::get;
using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace vast;

TEST(default construction) {
  type t;
  CHECK(!t);
  CHECK(!holds_alternative<boolean_type>(t));
}

TEST(construction) {
  auto s = string_type{};
  auto t = type{s};
  CHECK(t);
  CHECK(holds_alternative<string_type>(t));
  CHECK(get_if<string_type>(&t) != nullptr);
}

TEST(assignment) {
  auto t = type{string_type{}};
  CHECK(t);
  CHECK(holds_alternative<string_type>(t));
  t = real_type{};
  CHECK(t);
  CHECK(holds_alternative<real_type>(t));
  t = {};
  CHECK(!t);
  CHECK(!holds_alternative<real_type>(t));
}

TEST(copying) {
  auto t = type{string_type{}};
  auto u = t;
  CHECK(holds_alternative<string_type>(u));
}

TEST(names) {
  type t;
  t = t.name("foo");
  CHECK(t.name().empty());
  t = type{string_type{}};
  t = t.name("foo");
  CHECK_EQUAL(t.name(), "foo");
}

TEST(attributes) {
  auto attrs = std::vector<attribute>{{"key", "value"}};
  type t;
  t = t.attributes(attrs);
  CHECK(t.attributes().empty());
  t = string_type{};
  t = t.attributes({{"key", "value"}});
  CHECK_EQUAL(t.attributes(), attrs);
}

TEST(equality comparison) {
  MESSAGE("type-erased comparison");
  CHECK(type{} == type{});
  CHECK(type{boolean_type{}} != type{});
  CHECK(type{boolean_type{}} == type{boolean_type{}});
  CHECK(type{boolean_type{}} != type{real_type{}});
  auto x = type{string_type{}};
  auto y = type{string_type{}};
  x = x.name("foo");
  CHECK(x != y);
  y = y.name("foo");
  CHECK(x == y);
  MESSAGE("concrete type comparison");
  CHECK(real_type{} == real_type{});
  CHECK(real_type{}.name("foo") != real_type{});
  CHECK(real_type{}.name("foo") == real_type{}.name("foo"));
  auto attrs = std::vector<attribute>{{"key", "value"}};
  CHECK(real_type{}.attributes(attrs) != real_type{});
  CHECK(real_type{}.attributes(attrs) == real_type{}.attributes(attrs));
}

TEST(less-than comparison) {
  CHECK(!(type{} < type{}));
  CHECK(!(real_type{} < real_type{}));
  auto x = string_type{}.name("a");
  auto y = string_type{}.name("b");
  CHECK(x < y);
  x = x.name("b");
  y = y.name("a");
  CHECK(x > y);
}

TEST(strict weak ordering) {
  std::vector<type> xs{string_type{}, address_type{}, pattern_type{}};
  std::vector<type> ys{string_type{}, pattern_type{}, address_type{}};
  std::sort(xs.begin(), xs.end());
  std::sort(ys.begin(), ys.end());
  CHECK(xs == ys);
}

TEST(introspection) {
  CHECK(is_complex(enumeration_type{}));
  CHECK(!is_basic(enumeration_type{}));
  CHECK(is_complex(vector_type{}));
  CHECK(is_container(vector_type{}));
  CHECK(is_recursive(vector_type{}));
  CHECK(is_complex(set_type{}));
  CHECK(is_container(set_type{}));
  CHECK(is_recursive(set_type{}));
  CHECK(is_complex(map_type{}));
  CHECK(is_container(map_type{}));
  CHECK(is_recursive(map_type{}));
  CHECK(is_recursive(record_type{}));
  CHECK(!is_container(record_type{}));
  CHECK(is_recursive(alias_type{}));
  CHECK(!is_container(alias_type{}));
}

TEST(type/data compatibility) {
  CHECK(compatible(address_type{}, in, subnet_type{}));
  CHECK(compatible(address_type{}, in, subnet{}));
  CHECK(compatible(subnet_type{}, in, subnet_type{}));
  CHECK(compatible(subnet_type{}, in, subnet{}));
}

TEST(serialization) {
  auto r = record_type{
    {"x", integer_type{}},
    {"y", address_type{}},
    {"z", real_type{}.attributes({{"key", "value"}})}
  };
  // Make it recursive.
  r = {
    {"a", map_type{string_type{}, port_type{}}},
    {"b", vector_type{boolean_type{}}.name("foo")},
    {"c", r}
  };
  r = r.name("foo");
  std::vector<char> buf;
  auto t0 = type{r};
  save(buf, t0);
  type t1;
  load(buf, t1);
  CHECK_EQUAL(t0, t1);
}

TEST(record range) {
  auto r = record_type{
    {"x", record_type{
            {"y", record_type{
                    {"z", integer_type{}},
                    {"k", boolean_type{}}
                  }},
            {"m", record_type{
                    {"y", record_type{{"a", address_type{}}}},
                    {"f", real_type{}}
                  }},
            {"b", boolean_type{}}
          }},
    {"y", record_type{{"b", boolean_type{}}}}
  };

  for (auto& i : record_type::each{r})
    if (i.offset == offset{0, 1, 0, 0})
      CHECK(i.key() == key{"x", "m", "y", "a"});
    else if (i.offset == offset{1, 0})
      CHECK(i.key() == key{"y", "b"});
}

TEST(record resolving) {
  auto r = record_type{
    {"x", integer_type{}},
    {"y", address_type{}},
    {"z", real_type{}}
  };
  // Make it recursive.
  r = {
    {"a", integer_type{}},
    {"b", count_type{}},
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
  auto x = record_type{
    {"x", record_type{
      {"y", record_type{
        {"z", integer_type{}},
        {"k", boolean_type{}}
      }},
      {"m", record_type{
        {"y", record_type{{"a", address_type{}}}},
        {"f", real_type{}}
      }},
      {"b", boolean_type{}}
    }},
    {"y", record_type{
      {"b", boolean_type{}}
    }}
  };
  auto y = record_type{{
    {"x.y.z", integer_type{}},
    {"x.y.k", boolean_type{}},
    {"x.m.y.a", address_type{}},
    {"x.m.f", real_type{}},
    {"x.b", boolean_type{}},
    {"y.b", boolean_type{}}
  }};
  auto f = flatten(x);
  CHECK(f == y);
  auto u = unflatten(f);
  CHECK(u == x);
}

TEST(record flat index computation) {
  auto x = record_type{
    {"x", record_type{
      {"y", record_type{
        {"z", integer_type{}}, // 0: x.y.z [0, 0, 0]
        {"k", boolean_type{}}  // 1: x.y.k [0, 0, 1]
      }},
      {"m", record_type{
        {"y", record_type{
          {"a", address_type{}}} // 2: x.m.y.a [0, 1, 0, 0]
        },
        {"f", real_type{}} // 3: x.m.f [0, 1, 1]
      }},
      {"b", boolean_type{}} // 4: x.b [0, 2]
    }},
    {"y", record_type{
      {"b", boolean_type{}} // 5: y.b [1, 0]
    }}
  };
  using os = caf::optional<size_t>;
  static const os invalid;
  CHECK_EQUAL(flat_size(x), 6u);
  CHECK_EQUAL(x.flat_index_at(offset({0, 0, 0})), os(0u));
  CHECK_EQUAL(x.flat_index_at(offset({0, 0, 1})), os(1u));
  CHECK_EQUAL(x.flat_index_at(offset({0, 1, 0, 0})), os(2u));
  CHECK_EQUAL(x.flat_index_at(offset({0, 1, 1})), os(3u));
  CHECK_EQUAL(x.flat_index_at(offset({0, 2})), os(4u));
  CHECK_EQUAL(x.flat_index_at(offset({1, 0})), os(5u));
  CHECK_EQUAL(x.flat_index_at(offset({0})), invalid);
  CHECK_EQUAL(x.flat_index_at(offset({0, 0})), invalid);
  CHECK_EQUAL(x.flat_index_at(offset({1})), invalid);
  CHECK_EQUAL(x.flat_index_at(offset({2})), invalid);
}

TEST(record symbol finding) {
  auto r = record_type{
    {"x", integer_type{}},
    {"y", address_type{}},
    {"z", real_type{}}
  };
  r = {
    {"a", integer_type{}},
    {"b", count_type{}},
    {"c", record_type{r}}
  };
  r = {
    {"a", integer_type{}},
    {"b", record_type{r}},
    {"c", count_type{}}
  };
  r = r.name("foo");
  // Record access by key.
  auto first = r.at(key{"a"});
  REQUIRE(first);
  CHECK(holds_alternative<integer_type>(*first));
  auto deep = r.at(key{"b", "c", "y"});
  REQUIRE(deep);
  CHECK(holds_alternative<address_type>(*deep));
  MESSAGE("prefix finding");
  // Since the type has a name, the prefix has the form "name.first.second".
  // E.g., a full key is foo.a for field 0 or foo.b.c.z for a nested field.
  auto o = r.find_prefix({"a"});
  REQUIRE_EQUAL(o.size(), 0u); // type starts with "foo", not "a"
  o = r.find_prefix({"foo", "a"});
  offset a{0};
  REQUIRE_EQUAL(o.size(), 1u);
  CHECK(o[0].first == a);
  o = r.find_prefix({"foo", "b", "a"});
  offset ba{1, 0};
  REQUIRE_EQUAL(o.size(), 1u);
  CHECK(o[0].first == ba);
  MESSAGE("suffix finding");
  o = r.find_suffix({"z"});
  offset z{1, 2, 2};
  REQUIRE_EQUAL(o.size(), 1u);
  CHECK(o[0].first == z);
  o = r.find_suffix({"c", "y"});
  offset cy{1, 2, 1};
  REQUIRE_EQUAL(o.size(), 1u);
  CHECK(o[0].first == cy);
  o = r.find_suffix({"a"});
  offset a0{0}, a1{1, 0};
  REQUIRE_EQUAL(o.size(), 2u);
  CHECK(o[0].first == a0);
  CHECK(o[1].first == a1);
  o = r.find_suffix({"c", "*"});
  offset c0{1, 2, 0}, c1{1, 2, 1}, c2{1, 2, 2};
  REQUIRE_EQUAL(o.size(), 3u);
  CHECK(o[0].first == c0);
  CHECK(o[1].first == c1);
  CHECK(o[2].first == c2);
}

TEST(congruence) {
  MESSAGE("basic");
  auto i = integer_type{};
  auto j = integer_type{};
  CHECK(i == j);
  i = i.name("i");
  j = j.name("j");
  CHECK(i != j);
  auto c = count_type{};
  c = c.name("c");
  CHECK(congruent(i, i));
  CHECK(congruent(i, j));
  CHECK(!congruent(i, c));
  MESSAGE("sets");
  auto s0 = set_type{i};
  auto s1 = set_type{j};
  auto s2 = set_type{c};
  CHECK(s0 != s1);
  CHECK(s0 != s2);
  CHECK(congruent(s0, s1));
  CHECK(!congruent(s1, s2));
  MESSAGE("records");
  auto r0 = record_type{
    {"a", address_type{}},
    {"b", boolean_type{}},
    {"c", count_type{}}};
  auto r1 = record_type{
    {"x", address_type{}},
    {"y", boolean_type{}},
    {"z", count_type{}}};
  CHECK(r0 != r1);
  CHECK(congruent(r0, r1));
  MESSAGE("aliases");
  auto a = alias_type{i};
  a = a.name("a");
  CHECK(type{a} != type{i});
  CHECK(congruent(a, i));
  a = alias_type{r0};
  a = a.name("r0");
  CHECK(type{a} != type{r0});
  CHECK(congruent(a, r0));
}

TEST(printable) {
  MESSAGE("basic types");
  CHECK_EQUAL(to_string(type{}), "none");
  CHECK_EQUAL(to_string(boolean_type{}), "bool");
  CHECK_EQUAL(to_string(integer_type{}), "int");
  CHECK_EQUAL(to_string(count_type{}), "count");
  CHECK_EQUAL(to_string(real_type{}), "real");
  CHECK_EQUAL(to_string(timespan_type{}), "duration");
  CHECK_EQUAL(to_string(timestamp_type{}), "time");
  CHECK_EQUAL(to_string(string_type{}), "string");
  CHECK_EQUAL(to_string(pattern_type{}), "pattern");
  CHECK_EQUAL(to_string(address_type{}), "addr");
  CHECK_EQUAL(to_string(subnet_type{}), "subnet");
  CHECK_EQUAL(to_string(port_type{}), "port");
  MESSAGE("enumeration_type");
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  CHECK_EQUAL(to_string(e), "enum {foo, bar, baz}");
  MESSAGE("container types");
  CHECK_EQUAL(to_string(vector_type{real_type{}}), "vector<real>");
  CHECK_EQUAL(to_string(set_type{boolean_type{}}), "set<bool>");
  auto b = boolean_type{};
  CHECK_EQUAL(to_string(map_type{count_type{}, b}), "map<count, bool>");
  auto r = record_type{{
        {"foo", b},
        {"bar", integer_type{}},
        {"baz", real_type{}}
      }};
  CHECK_EQUAL(to_string(r), "record{foo: bool, bar: int, baz: real}");
  MESSAGE("alias");
  auto a = alias_type{real_type{}};
  CHECK_EQUAL(to_string(a), "real"); // haul through
  a = a.name("foo");
  CHECK_EQUAL(to_string(a), "real");
  CHECK_EQUAL(to_string(type{a}), "foo");
  MESSAGE("type");
  auto t = type{};
  CHECK_EQUAL(to_string(t), "none");
  t = e;
  CHECK_EQUAL(to_string(t), "enum {foo, bar, baz}");
  MESSAGE("attributes");
  auto attr = attribute{"foo", "bar"};
  CHECK_EQUAL(to_string(attr), "&foo=bar");
  attr = {"skip"};
  CHECK_EQUAL(to_string(attr), "&skip");
  // Attributes on types.
  auto s = set_type{port_type{}};
  s = s.attributes({attr, {"tokenize", "/rx/"}});
  CHECK_EQUAL(to_string(s), "set<port> &skip &tokenize=/rx/");
  // Nested types
  t = s;
  t = t.attributes({attr});
  t = map_type{count_type{}, t};
  CHECK_EQUAL(to_string(t), "map<count, set<port> &skip>");
  MESSAGE("signature");
  t = t.name("jells");
  std::string sig;
  CHECK(printers::type<policy::signature>(sig, t));
  CHECK_EQUAL(sig, "jells = map<count, set<port> &skip>");
}

TEST(parseable) {
  type t;
  MESSAGE("basic");
  CHECK(parsers::type("bool", t));
  CHECK(t == boolean_type{});
  CHECK(parsers::type("string", t));
  CHECK(t == string_type{});
  CHECK(parsers::type("addr", t));
  CHECK(t == address_type{});
  MESSAGE("enum");
  CHECK(parsers::type("enum{foo, bar, baz}", t));
  CHECK(t == enumeration_type{{"foo", "bar", "baz"}});
  MESSAGE("container");
  CHECK(parsers::type("vector<real>", t));
  CHECK(t == type{vector_type{real_type{}}});
  CHECK(parsers::type("set<port>", t));
  CHECK(t == type{set_type{port_type{}}});
  CHECK(parsers::type("map<count, bool>", t));
  CHECK(t == type{map_type{count_type{}, boolean_type{}}});
  MESSAGE("recursive");
  auto str = "record{r: record{a: addr, i: record{b: bool}}}"s;
  CHECK(parsers::type(str, t));
  auto r = record_type{
    {"r", record_type{
      {"a", address_type{}},
      {"i", record_type{{"b", boolean_type{}}}}
    }}
  };
  CHECK_EQUAL(t, r);
  MESSAGE("symbol table");
  auto foo = boolean_type{};
  foo = foo.name("foo");
  auto symbols = type_table{{"foo", foo}};
  auto p = type_parser{std::addressof(symbols)}; // overloaded operator&
  CHECK(p("foo", t));
  CHECK(t == foo);
  CHECK(p("vector<foo>", t));
  CHECK(t == type{vector_type{foo}});
  CHECK(p("set<foo>", t));
  CHECK(t == type{set_type{foo}});
  CHECK(p("map<foo, foo>", t));
  CHECK(t == type{map_type{foo, foo}});
  MESSAGE("record");
  CHECK(p("record{x: int, y: string, z: foo}", t));
  r = record_type{
    {"x", integer_type{}},
    {"y", string_type{}},
    {"z", foo}
  };
  CHECK(t == type{r});
  MESSAGE("attributes");
  // Single attribute.
  CHECK(p("string &skip", t));
  type u = string_type{}.attributes({{"skip"}});
  CHECK_EQUAL(t, u);
  // Two attributes, even though these ones don't make sense together.
  CHECK(p("real &skip &default=\"x \\\" x\"", t));
  u = real_type{}.attributes({{"skip"}, {"default", "x \" x"}});
  CHECK_EQUAL(t, u);
  // Attributes in types of record fields.
  CHECK(p("record{x: int &skip, y: string &default=\"Y\", z: foo}", t));
  r = record_type{
    {"x", integer_type{}.attributes({{"skip"}})},
    {"y", string_type{}.attributes({{"default", "Y"}})},
    {"z", foo}
  };
  CHECK_EQUAL(t, r);
}

TEST(hashable) {
  auto hash = [&](auto&& x) {
    return uhash<xxhash64>{}(x);
  };
  auto x = type{};
  CHECK_EQUAL(hash(x), 17241709254077376921ul);
  x = boolean_type{};
  CHECK_EQUAL(hash(x), 12612883901365648434ul);
  x = record_type{
    {"x", integer_type{}},
    {"y", string_type{}},
    {"z", vector_type{real_type{}}}
  };
  CHECK_EQUAL(hash(x), 7882356540908153692ul);
}

TEST(json) {
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  e = e.name("e");
  auto t = map_type{boolean_type{}, count_type{}};
  t = t.name("bit_table");
  auto r = record_type{
    {"x", address_type{}.attributes({{"skip"}})},
    {"y", boolean_type{}.attributes({{"default", "F"}})},
    {"z", record_type{{"inner", e}}}
  };
  r = r.name("foo");
  auto expected = R"__({
  "name": "foo",
  "kind": "record",
  "structure": {
    "x": {
      "name": "",
      "kind": "address",
      "structure": null,
      "attributes": {
        "skip": null
      }
    },
    "y": {
      "name": "",
      "kind": "bool",
      "structure": null,
      "attributes": {
        "default": "F"
      }
    },
    "z": {
      "name": "",
      "kind": "record",
      "structure": {
        "inner": {
          "name": "e",
          "kind": "enumeration",
          "structure": [
            "foo",
            "bar",
            "baz"
          ],
          "attributes": {}
        }
      },
      "attributes": {}
    }
  },
  "attributes": {}
})__";
  CHECK_EQUAL(to_string(to_json(type{r})), expected);
}
