//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type

#include "vast/type.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/data.hpp"

#include <string_view>

#include "type_test.hpp"

using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace vast;

FIXTURE_SCOPE(type_tests, fixtures::deterministic_actor_system)

TEST(default construction) {
  type t;
  CHECK(!t);
  CHECK(!holds_alternative<bool_type>(t));
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
  auto u = type{none_type{}};
  CHECK(u);
  CHECK(holds_alternative<none_type>(u));
}

TEST(copying) {
  auto t = type{string_type{}};
  auto u = t;
  CHECK(holds_alternative<string_type>(u));
}

TEST(names) {
  type t;
  t.name("foo");
  CHECK(t.name().empty());
  t = type{string_type{}};
  t.name("foo");
  CHECK_EQUAL(t.name(), "foo");
}

TEST(attributes) {
  auto attrs = std::vector<attribute>{{"key", "value"}};
  type t;
  t.attributes(attrs);
  CHECK(t.attributes().empty());
  t = string_type{};
  t.attributes({{"key", "value"}});
  CHECK_EQUAL(t.attributes(), attrs);
}

TEST(equality comparison) {
  MESSAGE("type-erased comparison");
  CHECK(type{} == type{});
  CHECK(type{bool_type{}} != type{});
  CHECK(type{bool_type{}} == type{bool_type{}});
  CHECK(type{bool_type{}} != type{real_type{}});
  auto x = type{string_type{}};
  auto y = type{string_type{}};
  x.name("foo");
  CHECK(x != y);
  y.name("foo");
  CHECK(x == y);
  MESSAGE("concrete type comparison");
  CHECK(real_type{} == real_type{});
  CHECK(real_type{}.name("foo") != real_type{});
  CHECK(real_type{}.name("foo") == real_type{}.name("foo"));
  auto attrs = std::vector<attribute>{{"key", "value"}};
  CHECK(real_type{}.attributes(attrs) != real_type{});
  CHECK(real_type{}.attributes(attrs) == real_type{}.attributes(attrs));
}

TEST(less - than comparison) {
  CHECK(!(type{} < type{}));
  CHECK(!(real_type{} < real_type{}));
  CHECK(string_type{}.name("a") < string_type{}.name("b"));
  CHECK(record_type{}.name("a") < record_type{}.name("b"));
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
  CHECK(is_complex(list_type{}));
  CHECK(is_container(list_type{}));
  CHECK(is_recursive(list_type{}));
  CHECK(is_complex(map_type{}));
  CHECK(is_container(map_type{}));
  CHECK(is_recursive(map_type{}));
  CHECK(is_recursive(record_type{}));
  CHECK(!is_container(record_type{}));
  CHECK(is_recursive(alias_type{}));
  CHECK(!is_container(alias_type{}));
}

TEST(type / data compatibility) {
  CHECK(compatible(address_type{}, relational_operator::in, subnet_type{}));
  CHECK(compatible(address_type{}, relational_operator::in, subnet{}));
  CHECK(compatible(subnet_type{}, relational_operator::in, subnet_type{}));
  CHECK(compatible(subnet_type{}, relational_operator::in, subnet{}));
}

TEST(serialization) {
  CHECK_ROUNDTRIP(type{});
  CHECK_ROUNDTRIP(none_type{});
  CHECK_ROUNDTRIP(bool_type{});
  CHECK_ROUNDTRIP(integer_type{});
  CHECK_ROUNDTRIP(count_type{});
  CHECK_ROUNDTRIP(real_type{});
  CHECK_ROUNDTRIP(duration_type{});
  CHECK_ROUNDTRIP(time_type{});
  CHECK_ROUNDTRIP(string_type{});
  CHECK_ROUNDTRIP(pattern_type{});
  CHECK_ROUNDTRIP(address_type{});
  CHECK_ROUNDTRIP(subnet_type{});
  CHECK_ROUNDTRIP(enumeration_type{});
  CHECK_ROUNDTRIP(list_type{});
  CHECK_ROUNDTRIP(map_type{});
  CHECK_ROUNDTRIP(record_type{});
  CHECK_ROUNDTRIP(alias_type{});
  CHECK_ROUNDTRIP(type{none_type{}});
  CHECK_ROUNDTRIP(type{bool_type{}});
  CHECK_ROUNDTRIP(type{integer_type{}});
  CHECK_ROUNDTRIP(type{count_type{}});
  CHECK_ROUNDTRIP(type{real_type{}});
  CHECK_ROUNDTRIP(type{duration_type{}});
  CHECK_ROUNDTRIP(type{time_type{}});
  CHECK_ROUNDTRIP(type{string_type{}});
  CHECK_ROUNDTRIP(type{pattern_type{}});
  CHECK_ROUNDTRIP(type{address_type{}});
  CHECK_ROUNDTRIP(type{subnet_type{}});
  CHECK_ROUNDTRIP(type{enumeration_type{}});
  CHECK_ROUNDTRIP(type{list_type{}});
  CHECK_ROUNDTRIP(type{map_type{}});
  CHECK_ROUNDTRIP(type{record_type{}});
  CHECK_ROUNDTRIP(type{alias_type{}});
  auto r = record_type{{"x", integer_type{}},
                       {"y", address_type{}},
                       {"z", real_type{}.attributes({{"key", "value"}})}};
  // Make it recursive.
  r = {{"a", map_type{string_type{}, count_type{}}},
       {"b", list_type{bool_type{}}.name("foo")},
       {"c", r}};
  r.name("foo");
  CHECK_ROUNDTRIP(r);
}

TEST(record range) {
  // clang-format off
  auto r = record_type{
    {"x", record_type{
            {"y", record_type{
                    {"z", integer_type{}},
                    {"k", bool_type{}}
                  }},
            {"m", record_type{
                    {"y", record_type{
                            {"a", address_type{}}}},
                    {"f", real_type{}}
                  }},
            {"b", bool_type{}}
          }},
    {"y", record_type{
            {"b", bool_type{}}}}
  };
  // clang-format on
  MESSAGE("check the number of leaves");
  CHECK_EQUAL(r.num_leaves(), 6u);
  MESSAGE("check types of record r");
  auto record_index = r.index();
  CHECK_EQUAL(at(r, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 0, 0), integer_type{});
  CHECK_EQUAL(at(r, 0, 0, 1), bool_type{});
  CHECK_EQUAL(at(r, 0, 1)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 1, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 1, 0, 0), address_type{});
  CHECK_EQUAL(at(r, 0, 1, 1), real_type{});
  CHECK_EQUAL(at(r, 0, 2), bool_type{});
  CHECK_EQUAL(at(r, 1)->index(), record_index);
  CHECK_EQUAL(at(r, 1, 0), bool_type{});
  MESSAGE("check keys of record r");
  std::vector<std::string> keys;
  for (auto& i : record_type::each{r})
    keys.emplace_back(i.key());
  std::vector<std::string> expected_keys{"x.y.z", "x.y.k", "x.m.y.a",
                                         "x.m.f", "x.b",   "y.b"};
  CHECK_EQUAL(keys, expected_keys);
}

TEST(record resolving) {
  auto r = record_type{{"a", integer_type{}},
                       {"b", count_type{}},
                       {"c", record_type{{"x", integer_type{}},
                                         {"y", address_type{}},
                                         {"z", real_type{}}}}};
  MESSAGE("top-level offset resolve");
  auto o = r.resolve("c");
  REQUIRE(o);
  CHECK_EQUAL(o->size(), 1u);
  CHECK_EQUAL(o->front(), 2u);
  MESSAGE("nested offset resolve");
  o = r.resolve("c.x");
  REQUIRE(o);
  CHECK_EQUAL(o->size(), 2u);
  CHECK_EQUAL(o->front(), 2u);
  CHECK_EQUAL(o->back(), 0u);
  o = r.resolve("c.x.absent");
  CHECK(!o);
  MESSAGE("top-level offset resolve");
  auto k = r.resolve(offset{2});
  REQUIRE(k);
  CHECK_EQUAL(*k, "c");
  MESSAGE("nested offset resolve");
  k = r.resolve(offset{2, 0});
  REQUIRE(k);
  CHECK_EQUAL(*k, "c.x");
}

TEST(record flattening / unflattening) {
  // clang-format off
  auto x = record_type{
    {"x", record_type{
      {"y", record_type{
        {"z", integer_type{}},
        {"k", bool_type{}}
      }},
      {"m", record_type{
        {"y", record_type{{"a", address_type{}}}},
        {"f", real_type{}}
      }},
      {"b", bool_type{}}
    }},
    {"y", record_type{
      {"b", bool_type{}}
    }}
  };
  auto y = record_type{{
    {"x.y.z", integer_type{}},
    {"x.y.k", bool_type{}},
    {"x.m.y.a", address_type{}},
    {"x.m.f", real_type{}},
    {"x.b", bool_type{}},
    {"y.b", bool_type{}}
  }};
  // clang-format on
  auto f = flatten(x);
  CHECK(f == y);
  auto u = unflatten(f);
  CHECK(u == x);
}

TEST(record flat index computation) {
  // clang-format off
  auto x = record_type{
    {"x", record_type{
      {"y", record_type{
        {"z", integer_type{}}, // 0: x.y.z [0, 0, 0]
        {"k", bool_type{}}  // 1: x.y.k [0, 0, 1]
      }},
      {"m", record_type{
        {"y", record_type{
          {"a", address_type{}}} // 2: x.m.y.a [0, 1, 0, 0]
        },
        {"f", real_type{}} // 3: x.m.f [0, 1, 1]
      }},
      {"b", bool_type{}} // 4: x.b [0, 2]
    }},
    {"y", record_type{
      {"b", bool_type{}} // 5: y.b [1, 0]
    }}
  };
  // clang-format on
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

record_type make_record() {
  auto r
    = record_type{{"a", integer_type{}},
                  {"b", record_type{{"a", integer_type{}},
                                    {"b", count_type{}},
                                    {"c", record_type{{"x", integer_type{}},
                                                      {"y", address_type{}},
                                                      {"z", real_type{}}}}}},
                  {"c", count_type{}}};
  r = r.name("foo");
  return r;
}

TEST(record symbol finding - exact) {
  const auto r = make_record();
  const auto f = flatten(r);
  auto first = r.at("a");
  REQUIRE(first);
  CHECK(holds_alternative<integer_type>(first->type));
  first = f.at("a");
  REQUIRE(first);
  CHECK(holds_alternative<integer_type>(first->type));
  auto deep = r.at("b.c.y");
  REQUIRE(deep);
  CHECK(holds_alternative<address_type>(deep->type));
  deep = f.at("b.c.y");
  REQUIRE(deep);
  CHECK(holds_alternative<address_type>(deep->type));
  auto rec = r.at("b");
  REQUIRE(rec);
  CHECK(holds_alternative<record_type>(rec->type));
  rec = f.at("b");
  // A flat record has no longer an internal record that can be accessed
  // directly. Hence the access fails.
  CHECK(!rec);
  rec = r.at("b.c");
  REQUIRE(rec);
  CHECK(holds_alternative<record_type>(rec->type));
  rec = f.at("b.c");
  CHECK(!rec);
}

using offset_keys = std::vector<offset>;

TEST(record symbol finding - suffix) {
  const auto r = make_record();
  const auto f = flatten(r);
  MESSAGE("single deep field");
  CHECK_EQUAL(r.find_suffix("c.y"), (offset_keys{{1, 2, 1}}));
  CHECK_EQUAL(f.find_suffix("c.y"), (offset_keys{{4}}));
  CHECK_EQUAL(r.find_suffix("z"), (offset_keys{{1, 2, 2}}));
  CHECK_EQUAL(f.find_suffix("z"), (offset_keys{{5}}));
  MESSAGE("multiple record fields");
  const auto a = offset_keys{{0}, {1, 0}};
  const auto a_flat = offset_keys{{0}, {1}};
  CHECK_EQUAL(r.find_suffix("a"), a);
  CHECK_EQUAL(f.find_suffix("a"), a_flat);
  MESSAGE("glob expression");
  const auto c = offset_keys{{1, 2, 0}, {1, 2, 1}, {1, 2, 2}};
  const auto c_flat = offset_keys{{3}, {4}, {5}};
  CHECK_EQUAL(r.find_suffix("c.*"), c);
  CHECK_EQUAL(f.find_suffix("c.*"), c_flat);
  MESSAGE("field that is also a record");
  CHECK_EQUAL(r.find_suffix("b"), (offset_keys{{1}, {1, 1}}));
  CHECK_EQUAL(f.find_suffix("b"), (offset_keys{{2}}));
  MESSAGE("record name is part of query");
  CHECK_EQUAL(r.find_suffix("foo.a"), (offset_keys{{0}}));
  CHECK_EQUAL(f.find_suffix("oo.b.c.y"), std::vector<offset>{});
}

TEST(different fields with same suffix) {
  auto r = record_type{{"zeek.client", string_type{}},
                       {"suricata.alert.flow.bytes_toclient", count_type{}}};
  auto suffixes = r.find_suffix("client");
  CHECK_EQUAL(suffixes.size(), 1ull);
}

TEST(same fields with different type) {
  auto r = record_type{{"client", string_type{}}, {"client", count_type{}}};
  auto suffixes = r.find_suffix("client");
  CHECK_EQUAL(suffixes.size(), 2ull);
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
  auto l0 = list_type{i};
  auto l1 = list_type{j};
  auto l2 = list_type{c};
  CHECK(l0 != l1);
  CHECK(l0 != l2);
  CHECK(congruent(l0, l1));
  CHECK(!congruent(l1, l2));
  MESSAGE("records");
  auto r0 = record_type{
    {"a", address_type{}}, {"b", bool_type{}}, {"c", count_type{}}};
  auto r1 = record_type{
    {"x", address_type{}}, {"y", bool_type{}}, {"z", count_type{}}};
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
  MESSAGE("unspecified type");
  CHECK(congruent(type{}, type{}));
  CHECK(!congruent(type{string_type{}}, type{}));
  CHECK(!congruent(type{}, type{string_type{}}));
}

TEST(subset) {
  MESSAGE("basic");
  auto i = integer_type{};
  auto j = integer_type{};
  CHECK(is_subset(i, j));
  i = i.name("i");
  j = j.name("j");
  CHECK(is_subset(i, j));
  auto c = count_type{};
  c = c.name("c");
  CHECK(is_subset(i, i));
  CHECK(is_subset(i, j));
  CHECK(!is_subset(i, c));
  MESSAGE("records");
  auto r0 = record_type{
    {"a", address_type{}}, {"b", bool_type{}}, {"c", count_type{}}};
  // Rename a field.
  auto r1 = record_type{
    {"a", address_type{}}, {"b", bool_type{}}, {"d", count_type{}}};
  // Add a field.
  auto r2 = record_type{{"a", address_type{}},
                        {"b", bool_type{}},
                        {"c", count_type{}},
                        {"d", count_type{}}};
  // Remove a field.
  auto r3 = record_type{{"a", address_type{}}, {"c", count_type{}}};
  // Change a field's type.
  auto r4 = record_type{
    {"a", pattern_type{}}, {"b", bool_type{}}, {"c", count_type{}}};
  CHECK(is_subset(r0, r0));
  CHECK(!is_subset(r0, r1));
  CHECK(is_subset(r0, r2));
  CHECK(!is_subset(r0, r3));
  CHECK(!is_subset(r0, r4));
}

#define TYPE_CHECK(type, value) CHECK(type_check(type, data{value}));

#define TYPE_CHECK_FAIL(type, value) CHECK(!type_check(type, data{value}));

TEST(type_check) {
  MESSAGE("basic types");
  TYPE_CHECK(none_type{}, caf::none);
  TYPE_CHECK(bool_type{}, false);
  TYPE_CHECK(integer_type{}, 42);
  TYPE_CHECK(count_type{}, 42u);
  TYPE_CHECK(real_type{}, 4.2);
  TYPE_CHECK(duration_type{}, duration{0});
  TYPE_CHECK(time_type{}, vast::time{});
  TYPE_CHECK(string_type{}, "foo"s);
  TYPE_CHECK(pattern_type{}, pattern{"foo"});
  TYPE_CHECK(address_type{}, address{});
  TYPE_CHECK(subnet_type{}, subnet{});
  MESSAGE("complex types");
  TYPE_CHECK(enumeration_type{{"foo"}}, enumeration{0});
  TYPE_CHECK_FAIL(enumeration_type{{"foo"}}, enumeration{1});
  MESSAGE("containers");
  TYPE_CHECK(list_type{integer_type{}}, list({1, 2, 3}));
  TYPE_CHECK(list_type{}, list({1, 2, 3}));
  TYPE_CHECK(list_type{}, list{});
  TYPE_CHECK(list_type{string_type{}}, list{});
  auto xs = map{{1, true}, {2, false}};
  TYPE_CHECK((map_type{integer_type{}, bool_type{}}), xs);
  TYPE_CHECK(map_type{}, xs);
  TYPE_CHECK(map_type{}, map{});
}

// clang-format off
TEST(type_check - nested record) {
  data x = record{
    {"x", "foo"},
    {"r", record{
      {"i", -42},
      {"r", record{
        {"u", 1001u}
      }},
    }},
    {"str", "x"},
    {"b", false}
  };
  type t = record_type{
    {"x", string_type{}},
    {"r", record_type{
      {"i", integer_type{}},
      {"r", record_type{
        {"u", count_type{}}
      }},
    }},
    {"str", string_type{}},
    {"b", bool_type{}}
  };
  CHECK(type_check(t, x));
}
// clang-format on

TEST(printable) {
  MESSAGE("basic types");
  CHECK_EQUAL(to_string(type{}), "none");
  CHECK_EQUAL(to_string(bool_type{}), "bool");
  CHECK_EQUAL(to_string(integer_type{}), "int");
  CHECK_EQUAL(to_string(count_type{}), "count");
  CHECK_EQUAL(to_string(real_type{}), "real");
  CHECK_EQUAL(to_string(duration_type{}), "duration");
  CHECK_EQUAL(to_string(time_type{}), "time");
  CHECK_EQUAL(to_string(string_type{}), "string");
  CHECK_EQUAL(to_string(pattern_type{}), "pattern");
  CHECK_EQUAL(to_string(address_type{}), "addr");
  CHECK_EQUAL(to_string(subnet_type{}), "subnet");
  MESSAGE("enumeration_type");
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  CHECK_EQUAL(to_string(e), "enum {foo, bar, baz}");
  MESSAGE("container types");
  CHECK_EQUAL(to_string(list_type{real_type{}}), "list<real>");
  auto b = bool_type{};
  CHECK_EQUAL(to_string(map_type{count_type{}, b}), "map<count, bool>");
  auto r
    = record_type{{{"foo", b}, {"bar", integer_type{}}, {"baz", real_type{}}}};
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
  CHECK_EQUAL(to_string(attr), "#foo=bar");
  attr = {"skip"};
  CHECK_EQUAL(to_string(attr), "#skip");
  // Attributes on types.
  auto s = list_type{bool_type{}};
  s = s.attributes({attr, {"tokenize", "/rx/"}});
  CHECK_EQUAL(to_string(s), "list<bool> #skip #tokenize=/rx/");
  // Nested types
  t = s;
  t.attributes({attr});
  t = map_type{count_type{}, t};
  CHECK_EQUAL(to_string(t), "map<count, list<bool> #skip>");
  MESSAGE("signature");
  t.name("jells");
  std::string sig;
  CHECK(printers::type<policy::signature>(sig, t));
  CHECK_EQUAL(sig, "jells = map<count, list<bool> #skip>");
}

TEST(parseable) {
  type t;
  MESSAGE("basic");
  CHECK(parsers::type("bool", t));
  CHECK(t == bool_type{});
  CHECK(parsers::type("string", t));
  CHECK(t == string_type{});
  CHECK(parsers::type("addr", t));
  CHECK(t == address_type{});
  MESSAGE("alias");
  CHECK(parsers::type("timestamp", t));
  CHECK_EQUAL(t, none_type{}.name("timestamp"));
  MESSAGE("enum");
  CHECK(parsers::type("enum{foo, bar, baz}", t));
  CHECK(t == enumeration_type{{"foo", "bar", "baz"}});
  MESSAGE("container");
  CHECK(parsers::type("list<real>", t));
  CHECK(t == type{list_type{real_type{}}});
  CHECK(parsers::type("map<count, bool>", t));
  CHECK(t == type{map_type{count_type{}, bool_type{}}});
  MESSAGE("record");
  auto str = R"__(record{"a b": addr, b: bool})__"sv;
  CHECK(parsers::type(str, t));
  // clang-format off
  auto r = record_type{
    {"a b", address_type{}},
    {"b", bool_type{}}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("recursive");
  str = "record{r: record{a: addr, i: record{b: bool}}}"sv;
  CHECK(parsers::type(str, t));
  // clang-format off
  r = record_type{
    {"r", record_type{
      {"a", address_type{}},
      {"i", record_type{{"b", bool_type{}}}}
    }}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("record algebra");
  // clang-format off
  r = record_type{
    {"", none_type{}.name("foo")},
    {"+", none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<type>("foo+bar")), r);
  CHECK_EQUAL(unbox(to<type>("foo + bar")), r);
  r.fields[1] = record_field{"-", record_type{{"bar", bool_type{}}}};
  CHECK_EQUAL(unbox(to<type>("foo-bar")), r);
  CHECK_EQUAL(unbox(to<type>("foo - bar")), r);
  str = "record{a: real} + bar"sv;
  // clang-format off
  r = record_type{
    {"", record_type{{"a", real_type{}}}},
    {"+", none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<type>(str)), r);
}

TEST(hashable) {
  auto hash = [&](auto&& x) { return uhash<xxhash64>{}(x); };
  CHECK_EQUAL(hash(type{}), 16682473723366582157ul);
  CHECK_EQUAL(hash(bool_type{}), 8019551906396149776ul);
  CHECK_EQUAL(hash(type{bool_type{}}), 693889673218214406ul);
  CHECK_NOT_EQUAL(hash(type{bool_type{}}), hash(bool_type{}));
  CHECK_NOT_EQUAL(hash(bool_type{}), hash(address_type{}));
  CHECK_NOT_EQUAL(hash(type{bool_type{}}), hash(type{address_type{}}));
  auto x = record_type{
    {"x", integer_type{}}, {"y", string_type{}}, {"z", list_type{real_type{}}}};
  CHECK_EQUAL(hash(x), 14779728178683051124ul);
  CHECK_EQUAL(to_digest(x), std::to_string(hash(type{x})));
}

TEST(json) {
  auto e = enumeration_type{{"foo", "bar", "baz"}};
  e = e.name("e");
  auto t = map_type{bool_type{}, count_type{}};
  t.name("bit_table");
  auto r = record_type{{"x", address_type{}.attributes({{"skip"}})},
                       {"y", bool_type{}.attributes({{"default", "F"}})},
                       {"z", record_type{{"inner", e}}}};
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
  CHECK_EQUAL(to_json(to_data(type{r})), expected);
}

FIXTURE_SCOPE_END()
