//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type

#include "vast/legacy_type.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/legacy_type.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/legacy_type.hpp"
#include "vast/concept/printable/vast/offset.hpp"
#include "vast/data.hpp"
#include "vast/hash/uhash.hpp"
#include "vast/hash/xxhash.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include <string_view>

#include "type_test.hpp"

using caf::get_if;
using caf::holds_alternative;

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace vast;

FIXTURE_SCOPE(type_tests, fixtures::deterministic_actor_system)

TEST(default construction) {
  legacy_type t;
  CHECK(!t);
  CHECK(!holds_alternative<legacy_bool_type>(t));
}

TEST(construction) {
  auto s = legacy_string_type{};
  auto t = legacy_type{s};
  CHECK(t);
  CHECK(holds_alternative<legacy_string_type>(t));
  CHECK(get_if<legacy_string_type>(&t) != nullptr);
}

TEST(assignment) {
  auto t = legacy_type{legacy_string_type{}};
  CHECK(t);
  CHECK(holds_alternative<legacy_string_type>(t));
  t = legacy_real_type{};
  CHECK(t);
  CHECK(holds_alternative<legacy_real_type>(t));
  t = {};
  CHECK(!t);
  CHECK(!holds_alternative<legacy_real_type>(t));
  auto u = legacy_type{legacy_none_type{}};
  CHECK(u);
  CHECK(holds_alternative<legacy_none_type>(u));
}

TEST(copying) {
  auto t = legacy_type{legacy_string_type{}};
  auto u = t;
  CHECK(holds_alternative<legacy_string_type>(u));
}

TEST(names) {
  legacy_type t;
  t.name("foo");
  CHECK(t.name().empty());
  t = legacy_type{legacy_string_type{}};
  t.name("foo");
  CHECK_EQUAL(t.name(), "foo");
}

TEST(attributes) {
  auto attrs = std::vector<attribute>{{"key", "value"}};
  legacy_type t;
  t.attributes(attrs);
  CHECK(t.attributes().empty());
  t = legacy_string_type{};
  t.attributes({{"key", "value"}});
  CHECK_EQUAL(t.attributes(), attrs);
}

TEST(equality comparison) {
  MESSAGE("type-erased comparison");
  CHECK(legacy_type{} == legacy_type{});
  CHECK(legacy_type{legacy_bool_type{}} != legacy_type{});
  CHECK(legacy_type{legacy_bool_type{}} == legacy_type{legacy_bool_type{}});
  CHECK(legacy_type{legacy_bool_type{}} != legacy_type{legacy_real_type{}});
  auto x = legacy_type{legacy_string_type{}};
  auto y = legacy_type{legacy_string_type{}};
  x.name("foo");
  CHECK(x != y);
  y.name("foo");
  CHECK(x == y);
  MESSAGE("concrete type comparison");
  CHECK(legacy_real_type{} == legacy_real_type{});
  CHECK(legacy_real_type{}.name("foo") != legacy_real_type{});
  CHECK(legacy_real_type{}.name("foo") == legacy_real_type{}.name("foo"));
  auto attrs = std::vector<attribute>{{"key", "value"}};
  CHECK(legacy_real_type{}.attributes(attrs) != legacy_real_type{});
  CHECK(legacy_real_type{}.attributes(attrs)
        == legacy_real_type{}.attributes(attrs));
}

TEST(less - than comparison) {
  CHECK(!(legacy_type{} < legacy_type{}));
  CHECK(!(legacy_real_type{} < legacy_real_type{}));
  CHECK(legacy_string_type{}.name("a") < legacy_string_type{}.name("b"));
  CHECK(legacy_record_type{}.name("a") < legacy_record_type{}.name("b"));
}

TEST(strict weak ordering) {
  std::vector<legacy_type> xs{legacy_string_type{}, legacy_address_type{},
                              legacy_pattern_type{}};
  std::vector<legacy_type> ys{legacy_string_type{}, legacy_pattern_type{},
                              legacy_address_type{}};
  std::sort(xs.begin(), xs.end());
  std::sort(ys.begin(), ys.end());
  CHECK(xs == ys);
}

TEST(introspection) {
  CHECK(is_complex(legacy_enumeration_type{}));
  CHECK(!is_basic(legacy_enumeration_type{}));
  CHECK(is_complex(legacy_list_type{}));
  CHECK(is_container(legacy_list_type{}));
  CHECK(is_recursive(legacy_list_type{}));
  CHECK(is_complex(legacy_map_type{}));
  CHECK(is_container(legacy_map_type{}));
  CHECK(is_recursive(legacy_map_type{}));
  CHECK(is_recursive(legacy_record_type{}));
  CHECK(!is_container(legacy_record_type{}));
  CHECK(is_recursive(legacy_alias_type{}));
  CHECK(!is_container(legacy_alias_type{}));
}

TEST(type / data compatibility) {
  CHECK(compatible(legacy_address_type{}, relational_operator::in,
                   legacy_subnet_type{}));
  CHECK(compatible(legacy_address_type{}, relational_operator::in, subnet{}));
  CHECK(compatible(legacy_subnet_type{}, relational_operator::in,
                   legacy_subnet_type{}));
  CHECK(compatible(legacy_subnet_type{}, relational_operator::in, subnet{}));
}

TEST(serialization) {
  CHECK_ROUNDTRIP(legacy_type{});
  CHECK_ROUNDTRIP(legacy_none_type{});
  CHECK_ROUNDTRIP(legacy_bool_type{});
  CHECK_ROUNDTRIP(legacy_integer_type{});
  CHECK_ROUNDTRIP(legacy_count_type{});
  CHECK_ROUNDTRIP(legacy_real_type{});
  CHECK_ROUNDTRIP(legacy_duration_type{});
  CHECK_ROUNDTRIP(legacy_time_type{});
  CHECK_ROUNDTRIP(legacy_string_type{});
  CHECK_ROUNDTRIP(legacy_pattern_type{});
  CHECK_ROUNDTRIP(legacy_address_type{});
  CHECK_ROUNDTRIP(legacy_subnet_type{});
  CHECK_ROUNDTRIP(legacy_enumeration_type{});
  CHECK_ROUNDTRIP(legacy_list_type{});
  CHECK_ROUNDTRIP(legacy_map_type{});
  CHECK_ROUNDTRIP(legacy_record_type{});
  CHECK_ROUNDTRIP(legacy_alias_type{});
  CHECK_ROUNDTRIP(legacy_type{legacy_none_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_bool_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_integer_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_count_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_real_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_duration_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_time_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_string_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_pattern_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_address_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_subnet_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_enumeration_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_list_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_map_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_record_type{}});
  CHECK_ROUNDTRIP(legacy_type{legacy_alias_type{}});
  auto r
    = legacy_record_type{{"x", legacy_integer_type{}},
                         {"y", legacy_address_type{}},
                         {"z", legacy_real_type{}.attributes({{"key", "valu"
                                                                      "e"}})}};
  // Make it recursive.
  r = {{"a", legacy_map_type{legacy_string_type{}, legacy_count_type{}}},
       {"b", legacy_list_type{legacy_bool_type{}}.name("foo")},
       {"c", r}};
  r.name("foo");
  CHECK_ROUNDTRIP(r);
}

TEST(record range) {
  // clang-format off
  auto r = legacy_record_type{
    {"x", legacy_record_type{
            {"y", legacy_record_type{
                    {"z", legacy_integer_type{}},
                    {"k", legacy_bool_type{}}
                  }},
            {"m", legacy_record_type{
                    {"y", legacy_record_type{
                            {"a", legacy_address_type{}}}},
                    {"f", legacy_real_type{}}
                  }},
            {"b", legacy_bool_type{}}
          }},
    {"y", legacy_record_type{
            {"b", legacy_bool_type{}}}}
  };
  // clang-format on
  MESSAGE("check the number of leaves");
  CHECK_EQUAL(r.num_leaves(), 6u);
  MESSAGE("check types of record r");
  auto record_index = r.index();
  CHECK_EQUAL(at(r, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 0, 0), legacy_integer_type{});
  CHECK_EQUAL(at(r, 0, 0, 1), legacy_bool_type{});
  CHECK_EQUAL(at(r, 0, 1)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 1, 0)->index(), record_index);
  CHECK_EQUAL(at(r, 0, 1, 0, 0), legacy_address_type{});
  CHECK_EQUAL(at(r, 0, 1, 1), legacy_real_type{});
  CHECK_EQUAL(at(r, 0, 2), legacy_bool_type{});
  CHECK_EQUAL(at(r, 1)->index(), record_index);
  CHECK_EQUAL(at(r, 1, 0), legacy_bool_type{});
  MESSAGE("check keys of record r");
  std::vector<std::string> keys;
  for (auto& i : legacy_record_type::each{r})
    keys.emplace_back(i.key());
  std::vector<std::string> expected_keys{"x.y.z", "x.y.k", "x.m.y.a",
                                         "x.m.f", "x.b",   "y.b"};
  CHECK_EQUAL(keys, expected_keys);
}

TEST(record resolving) {
  auto r
    = legacy_record_type{{"a", legacy_integer_type{}},
                         {"b", legacy_count_type{}},
                         {"c", legacy_record_type{{"x", legacy_integer_type{}},
                                                  {"y", legacy_address_type{}},
                                                  {"z", legacy_real_type{}}}}};
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

TEST(record flattening) {
  // clang-format off
  auto x = legacy_record_type{
    {"x", legacy_record_type{
      {"y", legacy_record_type{
        {"z", legacy_integer_type{}},
        {"k", legacy_bool_type{}}
      }},
      {"m", legacy_record_type{
        {"y", legacy_record_type{{"a", legacy_address_type{}}}},
        {"f", legacy_real_type{}}
      }},
      {"b", legacy_bool_type{}}
    }},
    {"y", legacy_record_type{
      {"b", legacy_bool_type{}}
    }}
  };
  auto y = legacy_record_type{{
    {"x.y.z", legacy_integer_type{}},
    {"x.y.k", legacy_bool_type{}},
    {"x.m.y.a", legacy_address_type{}},
    {"x.m.f", legacy_real_type{}},
    {"x.b", legacy_bool_type{}},
    {"y.b", legacy_bool_type{}}
  }};
  // clang-format on
  auto f = flatten(x);
  CHECK(f == y);
}

TEST(record flat index computation) {
  // clang-format off
  auto x = legacy_record_type{
    {"x", legacy_record_type{
      {"y", legacy_record_type{
        {"z", legacy_integer_type{}}, // 0: x.y.z [0, 0, 0]
        {"k", legacy_bool_type{}}  // 1: x.y.k [0, 0, 1]
      }},
      {"m", legacy_record_type{
        {"y", legacy_record_type{
          {"a", legacy_address_type{}}} // 2: x.m.y.a [0, 1, 0, 0]
        },
        {"f", legacy_real_type{}} // 3: x.m.f [0, 1, 1]
      }},
      {"b", legacy_bool_type{}} // 4: x.b [0, 2]
    }},
    {"y", legacy_record_type{
      {"b", legacy_bool_type{}} // 5: y.b [1, 0]
    }}
  };
  // clang-format on
  using os = std::optional<size_t>;
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

legacy_record_type make_record() {
  auto r = legacy_record_type{
    {"a", legacy_integer_type{}},
    {"b",
     legacy_record_type{{"a", legacy_integer_type{}},
                        {"b", legacy_count_type{}},
                        {"c", legacy_record_type{{"x", legacy_integer_type{}},
                                                 {"y", legacy_address_type{}},
                                                 {"z", legacy_real_type{}}}}}},
    {"c", legacy_count_type{}}};
  r = r.name("foo");
  return r;
}

TEST(record symbol finding - exact) {
  const auto r = make_record();
  const auto f = flatten(r);
  auto first = r.at("a");
  REQUIRE(first);
  CHECK(holds_alternative<legacy_integer_type>(first->type));
  first = f.at("a");
  REQUIRE(first);
  CHECK(holds_alternative<legacy_integer_type>(first->type));
  auto deep = r.at("b.c.y");
  REQUIRE(deep);
  CHECK(holds_alternative<legacy_address_type>(deep->type));
  deep = f.at("b.c.y");
  REQUIRE(deep);
  CHECK(holds_alternative<legacy_address_type>(deep->type));
  auto rec = r.at("b");
  REQUIRE(rec);
  CHECK(holds_alternative<legacy_record_type>(rec->type));
  rec = f.at("b");
  // A flat record has no longer an internal record that can be accessed
  // directly. Hence the access fails.
  CHECK(!rec);
  rec = r.at("b.c");
  REQUIRE(rec);
  CHECK(holds_alternative<legacy_record_type>(rec->type));
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
  auto r = legacy_record_type{{"zeek.client", legacy_string_type{}},
                              {"suricata.alert.flow.bytes_toclient",
                               legacy_count_type{}}};
  auto suffixes = r.find_suffix("client");
  CHECK_EQUAL(suffixes.size(), 1ull);
}

TEST(same fields with different type) {
  auto r = legacy_record_type{{"client", legacy_string_type{}},
                              {"client", legacy_count_type{}}};
  auto suffixes = r.find_suffix("client");
  CHECK_EQUAL(suffixes.size(), 2ull);
}

TEST(congruence) {
  MESSAGE("basic");
  auto i = legacy_integer_type{};
  auto j = legacy_integer_type{};
  CHECK(i == j);
  i = i.name("i");
  j = j.name("j");
  CHECK(i != j);
  auto c = legacy_count_type{};
  c = c.name("c");
  CHECK(congruent(i, i));
  CHECK(congruent(i, j));
  CHECK(!congruent(i, c));
  MESSAGE("sets");
  auto l0 = legacy_list_type{i};
  auto l1 = legacy_list_type{j};
  auto l2 = legacy_list_type{c};
  CHECK(l0 != l1);
  CHECK(l0 != l2);
  CHECK(congruent(l0, l1));
  CHECK(!congruent(l1, l2));
  MESSAGE("records");
  auto r0 = legacy_record_type{{"a", legacy_address_type{}},
                               {"b", legacy_bool_type{}},
                               {"c", legacy_count_type{}}};
  auto r1 = legacy_record_type{{"x", legacy_address_type{}},
                               {"y", legacy_bool_type{}},
                               {"z", legacy_count_type{}}};
  CHECK(r0 != r1);
  CHECK(congruent(r0, r1));
  MESSAGE("aliases");
  auto a = legacy_alias_type{i};
  a = a.name("a");
  CHECK(legacy_type{a} != legacy_type{i});
  CHECK(congruent(a, i));
  a = legacy_alias_type{r0};
  a = a.name("r0");
  CHECK(legacy_type{a} != legacy_type{r0});
  CHECK(congruent(a, r0));
  MESSAGE("unspecified type");
  CHECK(congruent(legacy_type{}, legacy_type{}));
  CHECK(!congruent(legacy_type{legacy_string_type{}}, legacy_type{}));
  CHECK(!congruent(legacy_type{}, legacy_type{legacy_string_type{}}));
}

TEST(subset) {
  MESSAGE("basic");
  auto i = legacy_integer_type{};
  auto j = legacy_integer_type{};
  CHECK(is_subset(i, j));
  i = i.name("i");
  j = j.name("j");
  CHECK(is_subset(i, j));
  auto c = legacy_count_type{};
  c = c.name("c");
  CHECK(is_subset(i, i));
  CHECK(is_subset(i, j));
  CHECK(!is_subset(i, c));
  MESSAGE("records");
  auto r0 = legacy_record_type{{"a", legacy_address_type{}},
                               {"b", legacy_bool_type{}},
                               {"c", legacy_count_type{}}};
  // Rename a field.
  auto r1 = legacy_record_type{{"a", legacy_address_type{}},
                               {"b", legacy_bool_type{}},
                               {"d", legacy_count_type{}}};
  // Add a field.
  auto r2 = legacy_record_type{{"a", legacy_address_type{}},
                               {"b", legacy_bool_type{}},
                               {"c", legacy_count_type{}},
                               {"d", legacy_count_type{}}};
  // Remove a field.
  auto r3 = legacy_record_type{{"a", legacy_address_type{}},
                               {"c", legacy_count_type{}}};
  // Change a field's type.
  auto r4 = legacy_record_type{{"a", legacy_pattern_type{}},
                               {"b", legacy_bool_type{}},
                               {"c", legacy_count_type{}}};
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
  TYPE_CHECK(legacy_none_type{}, caf::none);
  TYPE_CHECK(legacy_bool_type{}, false);
  TYPE_CHECK(legacy_integer_type{}, integer{42});
  TYPE_CHECK(legacy_count_type{}, 42u);
  TYPE_CHECK(legacy_real_type{}, 4.2);
  TYPE_CHECK(legacy_duration_type{}, duration{0});
  TYPE_CHECK(legacy_time_type{}, vast::time{});
  TYPE_CHECK(legacy_string_type{}, "foo"s);
  TYPE_CHECK(legacy_pattern_type{}, pattern{"foo"});
  TYPE_CHECK(legacy_address_type{}, address{});
  TYPE_CHECK(legacy_subnet_type{}, subnet{});
  MESSAGE("complex types");
  TYPE_CHECK(legacy_enumeration_type{{"foo"}}, enumeration{0});
  TYPE_CHECK_FAIL(legacy_enumeration_type{{"foo"}}, enumeration{1});
  MESSAGE("containers");
  TYPE_CHECK(legacy_list_type{legacy_integer_type{}},
             list({integer{1}, integer{2}, integer{3}}));
  TYPE_CHECK(legacy_list_type{}, list({integer{1}, integer{2}, integer{3}}));
  TYPE_CHECK(legacy_list_type{}, list{});
  TYPE_CHECK(legacy_list_type{legacy_string_type{}}, list{});
  auto xs = map{{integer{1}, true}, {integer{2}, false}};
  TYPE_CHECK((legacy_map_type{legacy_integer_type{}, legacy_bool_type{}}), xs);
  TYPE_CHECK(legacy_map_type{}, xs);
  TYPE_CHECK(legacy_map_type{}, map{});
}

// clang-format off
TEST(type_check - nested record) {
  data x = record{
    {"x", "foo"},
    {"r", record{
      {"i", integer{-42}},
      {"r", record{
        {"u", 1001u}
      }},
    }},
    {"str", "x"},
    {"b", false}
  };
  legacy_type t = legacy_record_type{
    {"x", legacy_string_type{}},
    {"r", legacy_record_type{
      {"i", legacy_integer_type{}},
      {"r", legacy_record_type{
        {"u", legacy_count_type{}}
      }},
    }},
    {"str", legacy_string_type{}},
    {"b", legacy_bool_type{}}
  };
  CHECK(type_check(t, x));
}
// clang-format on

TEST(printable) {
  MESSAGE("basic types");
  CHECK_EQUAL(to_string(legacy_type{}), "none");
  CHECK_EQUAL(to_string(legacy_bool_type{}), "bool");
  CHECK_EQUAL(to_string(legacy_integer_type{}), "int");
  CHECK_EQUAL(to_string(legacy_count_type{}), "count");
  CHECK_EQUAL(to_string(legacy_real_type{}), "real");
  CHECK_EQUAL(to_string(legacy_duration_type{}), "duration");
  CHECK_EQUAL(to_string(legacy_time_type{}), "time");
  CHECK_EQUAL(to_string(legacy_string_type{}), "string");
  CHECK_EQUAL(to_string(legacy_pattern_type{}), "pattern");
  CHECK_EQUAL(to_string(legacy_address_type{}), "addr");
  CHECK_EQUAL(to_string(legacy_subnet_type{}), "subnet");
  MESSAGE("legacy_enumeration_type");
  auto e = legacy_enumeration_type{{"foo", "bar", "baz"}};
  CHECK_EQUAL(to_string(e), "enum {foo, bar, baz}");
  MESSAGE("container types");
  CHECK_EQUAL(to_string(legacy_list_type{legacy_real_type{}}), "list<real>");
  auto b = legacy_bool_type{};
  CHECK_EQUAL(to_string(legacy_map_type{legacy_count_type{}, b}), "map<count, "
                                                                  "bool>");
  auto r = legacy_record_type{
    {{"foo", b}, {"bar", legacy_integer_type{}}, {"baz", legacy_real_type{}}}};
  CHECK_EQUAL(to_string(r), "record{foo: bool, bar: int, baz: real}");
  MESSAGE("alias");
  auto a = legacy_alias_type{legacy_real_type{}};
  CHECK_EQUAL(to_string(a), "real"); // haul through
  a = a.name("foo");
  CHECK_EQUAL(to_string(a), "real");
  CHECK_EQUAL(to_string(legacy_type{a}), "foo");
  MESSAGE("type");
  auto t = legacy_type{};
  CHECK_EQUAL(to_string(t), "none");
  t = e;
  CHECK_EQUAL(to_string(t), "enum {foo, bar, baz}");
  MESSAGE("attributes");
  auto attr = attribute{"foo", "bar"};
  CHECK_EQUAL(to_string(attr), "#foo=bar");
  attr = {"skip"};
  CHECK_EQUAL(to_string(attr), "#skip");
  // Attributes on types.
  auto s = legacy_list_type{legacy_bool_type{}};
  s = s.attributes({attr, {"tokenize", "/rx/"}});
  CHECK_EQUAL(to_string(s), "list<bool> #skip #tokenize=/rx/");
  // Nested types
  t = s;
  t.attributes({attr});
  t = legacy_map_type{legacy_count_type{}, t};
  CHECK_EQUAL(to_string(t), "map<count, list<bool> #skip>");
  MESSAGE("signature");
  t.name("jells");
  std::string sig;
  CHECK(printers::type<policy::signature>(sig, t));
  CHECK_EQUAL(sig, "jells = map<count, list<bool> #skip>");
}

TEST(parseable) {
  legacy_type t;
  MESSAGE("basic");
  CHECK(parsers::type("bool", t));
  CHECK(t == legacy_bool_type{});
  CHECK(parsers::type("string", t));
  CHECK(t == legacy_string_type{});
  CHECK(parsers::type("addr", t));
  CHECK(t == legacy_address_type{});
  MESSAGE("alias");
  CHECK(parsers::type("timestamp", t));
  CHECK_EQUAL(t, legacy_none_type{}.name("timestamp"));
  MESSAGE("enum");
  CHECK(parsers::type("enum{foo, bar, baz}", t));
  CHECK(t == legacy_enumeration_type{{"foo", "bar", "baz"}});
  MESSAGE("container");
  CHECK(parsers::type("list<real>", t));
  CHECK(t == legacy_type{legacy_list_type{legacy_real_type{}}});
  CHECK(parsers::type("map<count, bool>", t));
  CHECK(
    t == legacy_type{legacy_map_type{legacy_count_type{}, legacy_bool_type{}}});
  MESSAGE("record");
  auto str = R"__(record{"a b": addr, b: bool})__"sv;
  CHECK(parsers::type(str, t));
  // clang-format off
  auto r = legacy_record_type{
    {"a b", legacy_address_type{}},
    {"b", legacy_bool_type{}}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("recursive");
  str = "record{r: record{a: addr, i: record{b: bool}}}"sv;
  CHECK(parsers::type(str, t));
  // clang-format off
  r = legacy_record_type{
    {"r", legacy_record_type{
      {"a", legacy_address_type{}},
      {"i", legacy_record_type{{"b", legacy_bool_type{}}}}
    }}
  };
  // clang-format on
  CHECK_EQUAL(t, r);
  MESSAGE("record algebra");
  // clang-format off
  r = legacy_record_type{
    {"", legacy_none_type{}.name("foo")},
    {"+", legacy_none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<legacy_type>("foo+bar")), r);
  CHECK_EQUAL(unbox(to<legacy_type>("foo + bar")), r);
  r.fields[1]
    = record_field{"-", legacy_record_type{{"bar", legacy_bool_type{}}}};
  CHECK_EQUAL(unbox(to<legacy_type>("foo-bar")), r);
  CHECK_EQUAL(unbox(to<legacy_type>("foo - bar")), r);
  str = "record{a: real} + bar"sv;
  // clang-format off
  r = legacy_record_type{
    {"", legacy_record_type{{"a", legacy_real_type{}}}},
    {"+", legacy_none_type{}.name("bar")}
  }.attributes({{"$algebra"}});
  // clang-format on
  CHECK_EQUAL(unbox(to<legacy_type>(str)), r);
}

TEST(hashable) {
  auto h = [&](auto&& x) {
    return vast::hash<xxh64>(x);
  };
  CHECK_EQUAL(h(legacy_type{}), 16682473723366582157ul);
  CHECK_EQUAL(h(legacy_bool_type{}), 8019551906396149776ul);
  CHECK_EQUAL(h(legacy_type{legacy_bool_type{}}), 693889673218214406ul);
  CHECK_NOT_EQUAL(h(legacy_type{legacy_bool_type{}}), h(legacy_bool_type{}));
  CHECK_NOT_EQUAL(h(legacy_bool_type{}), h(legacy_address_type{}));
  CHECK_NOT_EQUAL(h(legacy_type{legacy_bool_type{}}),
                  h(legacy_type{legacy_address_type{}}));
  auto x = legacy_record_type{{"x", legacy_integer_type{}},
                              {"y", legacy_string_type{}},
                              {"z", legacy_list_type{legacy_real_type{}}}};
  CHECK_EQUAL(h(x), 14779728178683051124ul);
  CHECK_EQUAL(to_digest(x), std::to_string(h(legacy_type{x})));
}

TEST(json) {
  auto e = legacy_enumeration_type{{"foo", "bar", "baz"}};
  e = e.name("e");
  auto t = legacy_map_type{legacy_bool_type{}, legacy_count_type{}};
  t.name("bit_table");
  auto r = legacy_record_type{
    {"x", legacy_address_type{}.attributes({{"skip"}})},
    {"y", legacy_bool_type{}.attributes({{"default", "F"}})},
    {"z", legacy_record_type{{"inner", e}}}};
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
  CHECK_EQUAL(to_json(to_data(legacy_type{r})), expected);
}

FIXTURE_SCOPE_END()
