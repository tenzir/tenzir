#include "vast/json.hpp"
#include "vast/load.hpp"
#include "vast/type.hpp"
#include "vast/save.hpp"
#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/concept/printable/vast/type.hpp"

#define SUITE type
#include "test.hpp"

using namespace std::string_literals;
using namespace vast;

TEST(default construction) {
  type t;
  CHECK(get_if<none_type>(t) != nullptr);
  CHECK(get_if<boolean_type>(t) == nullptr);
}

TEST(construction and assignment) {
  auto s = string_type{};
  type t;
  CHECK(get_if<none_type>(t));
  t = s;
  CHECK(get_if<string_type>(t));
  t = vector_type{integer_type{}};
  auto v = get_if<vector_type>(t);
  REQUIRE(v);
  CHECK(get_if<integer_type>(v->value_type));
}

TEST(name) {
  auto v = vector_type{integer_type{}};
  auto h0 = uhash<type::hasher>{}(v);
  v.name("foo");
  auto h1 = uhash<type::hasher>{}(v);
  CHECK_NOT_EQUAL(h0, h1);
  v.name("");
  h1 = uhash<type::hasher>{}(v);
  CHECK_EQUAL(h0, h1);
  v.name("bar");
  auto t = type{v};
  CHECK_EQUAL(t.name(), "bar");
}

TEST(attributes) {
  auto t = set_type{};
  t.attributes().emplace_back("foo", "bar");
  REQUIRE_EQUAL(t.attributes().size(), 1u);
  CHECK_EQUAL(t.attributes()[0].key, "foo"s);
  CHECK_EQUAL(t.attributes()[0].value, "bar"s);
}

TEST(equality comparison) {
  auto t0 = vector_type{boolean_type{}};
  auto t1 = vector_type{boolean_type{}};
  CHECK(t0 == t1);
  CHECK(!(t0 != t1));
  t0.value_type = count_type{};
  CHECK(t0 != t1);
  CHECK(!(t0 == t1));
  t1.value_type = count_type{};
  CHECK(t0 == t1);
}

TEST(introspection) {
  CHECK(!is_recursive(enumeration_type{}));
  CHECK(is_recursive(vector_type{}));
  CHECK(is_recursive(set_type{}));
  CHECK(is_recursive(table_type{}));
  CHECK(is_recursive(record_type{}));
  CHECK(is_recursive(alias_type{}));
}

TEST(serialization) {
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
  r.name("foo");
  std::vector<char> buf;
  auto t0 = type{r};
  save(buf, t0);
  type t1;
  load(buf, t1);
  CHECK(t0 == t1);
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
    {"y", record_type{{"b", boolean_type{}}}}
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
  r.name("foo");
  // Record access by key.
  auto first = r.at(key{"a"});
  REQUIRE(first);
  CHECK(get_if<integer_type>(*first));
  auto deep = r.at(key{"b", "c", "y"});
  REQUIRE(deep);
  CHECK(get_if<address_type>(*deep));
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
  auto i = integer_type{};
  auto j = integer_type{};
  CHECK(i == j);
  i.name("i");
  j.name("j");
  CHECK(i != j);
  auto c = count_type{};
  c.name("c");
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
  CHECK(! congruent(s1, s2));
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
  a.name("a");
  CHECK(type{a} != type{i});
  CHECK(congruent(a, i));
  a = alias_type{r0};
  a.name("r0");
  CHECK(type{a} != type{r0});
  CHECK(congruent(a, r0));
}

TEST(printable) {
  // Plain types
  MESSAGE("basic types");
  CHECK_EQUAL(to_string(none_type{}), "none");
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
  MESSAGE("enumeration");
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  CHECK_EQUAL(to_string(e), "enum {foo, bar, baz}");
  MESSAGE("container types");
  CHECK_EQUAL(to_string(vector_type{real_type{}}), "vector<real>");
  CHECK_EQUAL(to_string(set_type{boolean_type{}}), "set<bool>");
  auto b = boolean_type{};
  CHECK_EQUAL(to_string(table_type{count_type{}, b}), "table<count, bool>");
  auto r = record_type{{
        {"foo", b},
        {"bar", integer_type{}},
        {"baz", real_type{}}
      }};
  CHECK_EQUAL(to_string(r), "record{foo: bool, bar: int, baz: real}");
  MESSAGE("alias");
  auto a = alias_type{real_type{}};
  CHECK_EQUAL(to_string(a), "real"); // haul through
  a.name("foo");
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
  s.attributes().emplace_back(attr);
  s.attributes().emplace_back("tokenize", "/rx/");
  CHECK_EQUAL(to_string(s), "set<port> &skip &tokenize=/rx/");
  // Nested types
  t = s;
  t.attributes().resize(1);
  t = table_type{count_type{}, t};
  CHECK_EQUAL(to_string(t), "table<count, set<port> &skip>");
  MESSAGE("signature");
  t.name("jells");
  std::string sig;
  CHECK(printers::type<policy::signature>(sig, t));
  CHECK_EQUAL(sig, "jells = table<count, set<port> &skip>");
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
  CHECK(parsers::type("table<count, bool>", t));
  CHECK(t == type{table_type{count_type{}, boolean_type{}}});
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
  foo.name("foo");
  auto symbols = type_table{{"foo", foo}};
  auto p = type_parser{std::addressof(symbols)}; // overloaded operator&
  CHECK(p("foo", t));
  CHECK(t == foo);
  CHECK(p("vector<foo>", t));
  CHECK(t == type{vector_type{foo}});
  CHECK(p("set<foo>", t));
  CHECK(t == type{set_type{foo}});
  CHECK(p("table<foo, foo>", t));
  CHECK(t == type{table_type{foo, foo}});
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

TEST(json) {
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  e.name("e");
  auto t = table_type{boolean_type{}, count_type{}};
  t.name("bit_table");
  auto r = record_type{
    {"x", address_type{}.attributes({{"skip"}})},
    {"y", boolean_type{}.attributes({{"default", "F"}})},
    {"z", record_type{{"inner", e}}}
  };
  r.name("foo");
  auto expected = R"__({
  "attributes": {},
  "kind": "record",
  "name": "foo",
  "structure": {
    "x": {
      "attributes": {
        "skip": null
      },
      "kind": "address",
      "name": "",
      "structure": null
    },
    "y": {
      "attributes": {
        "default": "F"
      },
      "kind": "bool",
      "name": "",
      "structure": null
    },
    "z": {
      "attributes": {},
      "kind": "record",
      "name": "",
      "structure": {
        "inner": {
          "attributes": {},
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
  CHECK_EQUAL(to_string(to_json(type{r})), expected);
}
