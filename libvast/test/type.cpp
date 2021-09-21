//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type

#include "vast/type.hpp"

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"
#include "vast/legacy_type.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <random>

namespace vast {

TEST(none_type) {
  static_assert(concrete_type<none_type>);
  static_assert(basic_type<none_type>);
  static_assert(!complex_type<none_type>);
  const auto t = type{};
  const auto nt = type{none_type{}};
  CHECK(!t);
  CHECK(!nt);
  CHECK_EQUAL(as_bytes(t), as_bytes(nt));
  CHECK(t == nt);
  CHECK(t <= nt);
  CHECK(t >= nt);
  CHECK_EQUAL(fmt::format("{}", t), "none");
  CHECK_EQUAL(fmt::format("{}", nt), "none");
  CHECK_EQUAL(fmt::format("{}", none_type{}), "none");
  CHECK(caf::holds_alternative<none_type>(t));
  CHECK(caf::holds_alternative<none_type>(nt));
  const auto lt = type{legacy_type{}};
  const auto lnt = type{legacy_none_type{}};
  CHECK(caf::holds_alternative<none_type>(lt));
  CHECK(caf::holds_alternative<none_type>(lnt));
}

TEST(bool_type) {
  static_assert(concrete_type<bool_type>);
  static_assert(basic_type<bool_type>);
  static_assert(!complex_type<bool_type>);
  const auto t = type{};
  const auto bt = type{bool_type{}};
  CHECK(bt);
  CHECK_EQUAL(as_bytes(bt), as_bytes(bool_type{}));
  CHECK(t != bt);
  CHECK(t < bt);
  CHECK(t <= bt);
  CHECK_EQUAL(fmt::format("{}", bt), "bool");
  CHECK_EQUAL(fmt::format("{}", bool_type{}), "bool");
  CHECK(!caf::holds_alternative<bool_type>(t));
  CHECK(caf::holds_alternative<bool_type>(bt));
  const auto lbt = type{legacy_bool_type{}};
  CHECK(caf::holds_alternative<bool_type>(lbt));
}

TEST(integer_type) {
  static_assert(concrete_type<integer_type>);
  static_assert(basic_type<integer_type>);
  static_assert(!complex_type<integer_type>);
  const auto t = type{};
  const auto it = type{integer_type{}};
  CHECK(it);
  CHECK_EQUAL(as_bytes(it), as_bytes(integer_type{}));
  CHECK(t != it);
  CHECK(t < it);
  CHECK(t <= it);
  CHECK_EQUAL(fmt::format("{}", it), "integer");
  CHECK_EQUAL(fmt::format("{}", integer_type{}), "integer");
  CHECK(!caf::holds_alternative<integer_type>(t));
  CHECK(caf::holds_alternative<integer_type>(it));
  const auto lit = type{legacy_integer_type{}};
  CHECK(caf::holds_alternative<integer_type>(lit));
}

TEST(count_type) {
  static_assert(concrete_type<count_type>);
  static_assert(basic_type<count_type>);
  static_assert(!complex_type<count_type>);
  const auto t = type{};
  const auto ct = type{count_type{}};
  CHECK(ct);
  CHECK_EQUAL(as_bytes(ct), as_bytes(count_type{}));
  CHECK(t != ct);
  CHECK(t < ct);
  CHECK(t <= ct);
  CHECK_EQUAL(fmt::format("{}", ct), "count");
  CHECK_EQUAL(fmt::format("{}", count_type{}), "count");
  CHECK(!caf::holds_alternative<count_type>(t));
  CHECK(caf::holds_alternative<count_type>(ct));
  const auto lct = type{legacy_count_type{}};
  CHECK(caf::holds_alternative<count_type>(lct));
}

TEST(real_type) {
  static_assert(concrete_type<real_type>);
  static_assert(basic_type<real_type>);
  static_assert(!complex_type<real_type>);
  const auto t = type{};
  const auto rt = type{real_type{}};
  CHECK(rt);
  CHECK_EQUAL(as_bytes(rt), as_bytes(real_type{}));
  CHECK(t != rt);
  CHECK(t < rt);
  CHECK(t <= rt);
  CHECK_EQUAL(fmt::format("{}", rt), "real");
  CHECK_EQUAL(fmt::format("{}", real_type{}), "real");
  CHECK(!caf::holds_alternative<real_type>(t));
  CHECK(caf::holds_alternative<real_type>(rt));
  const auto lrt = type{legacy_real_type{}};
  CHECK(caf::holds_alternative<real_type>(lrt));
}

TEST(duration_type) {
  static_assert(concrete_type<duration_type>);
  static_assert(basic_type<duration_type>);
  static_assert(!complex_type<duration_type>);
  const auto t = type{};
  const auto dt = type{duration_type{}};
  CHECK(dt);
  CHECK_EQUAL(as_bytes(dt), as_bytes(duration_type{}));
  CHECK(t != dt);
  CHECK(t < dt);
  CHECK(t <= dt);
  CHECK_EQUAL(fmt::format("{}", dt), "duration");
  CHECK_EQUAL(fmt::format("{}", duration_type{}), "duration");
  CHECK(!caf::holds_alternative<duration_type>(t));
  CHECK(caf::holds_alternative<duration_type>(dt));
  const auto ldt = type{legacy_duration_type{}};
  CHECK(caf::holds_alternative<duration_type>(ldt));
}

TEST(time_type) {
  static_assert(concrete_type<time_type>);
  static_assert(basic_type<time_type>);
  static_assert(!complex_type<time_type>);
  const auto t = type{};
  const auto tt = type{time_type{}};
  CHECK(tt);
  CHECK_EQUAL(as_bytes(tt), as_bytes(time_type{}));
  CHECK(t != tt);
  CHECK(t < tt);
  CHECK(t <= tt);
  CHECK_EQUAL(fmt::format("{}", tt), "time");
  CHECK_EQUAL(fmt::format("{}", time_type{}), "time");
  CHECK(!caf::holds_alternative<time_type>(t));
  CHECK(caf::holds_alternative<time_type>(tt));
  const auto ltt = type{legacy_time_type{}};
  CHECK(caf::holds_alternative<time_type>(ltt));
}

TEST(string_type) {
  static_assert(concrete_type<string_type>);
  static_assert(basic_type<string_type>);
  static_assert(!complex_type<string_type>);
  const auto t = type{};
  const auto st = type{string_type{}};
  CHECK(st);
  CHECK_EQUAL(as_bytes(st), as_bytes(string_type{}));
  CHECK(t != st);
  CHECK(t < st);
  CHECK(t <= st);
  CHECK_EQUAL(fmt::format("{}", st), "string");
  CHECK_EQUAL(fmt::format("{}", string_type{}), "string");
  CHECK(!caf::holds_alternative<string_type>(t));
  CHECK(caf::holds_alternative<string_type>(st));
  const auto lst = type{legacy_string_type{}};
  CHECK(caf::holds_alternative<string_type>(lst));
}

TEST(pattern_type) {
  static_assert(concrete_type<pattern_type>);
  static_assert(basic_type<pattern_type>);
  static_assert(!complex_type<pattern_type>);
  const auto t = type{};
  const auto pt = type{pattern_type{}};
  CHECK(pt);
  CHECK_EQUAL(as_bytes(pt), as_bytes(pattern_type{}));
  CHECK(t != pt);
  CHECK(t < pt);
  CHECK(t <= pt);
  CHECK_EQUAL(fmt::format("{}", pt), "pattern");
  CHECK_EQUAL(fmt::format("{}", pattern_type{}), "pattern");
  CHECK(!caf::holds_alternative<pattern_type>(t));
  CHECK(caf::holds_alternative<pattern_type>(pt));
  const auto lpt = type{legacy_pattern_type{}};
  CHECK(caf::holds_alternative<pattern_type>(lpt));
}

TEST(address_type) {
  static_assert(concrete_type<address_type>);
  static_assert(basic_type<address_type>);
  static_assert(!complex_type<address_type>);
  const auto t = type{};
  const auto at = type{address_type{}};
  CHECK(at);
  CHECK_EQUAL(as_bytes(at), as_bytes(address_type{}));
  CHECK(t != at);
  CHECK(t < at);
  CHECK(t <= at);
  CHECK_EQUAL(fmt::format("{}", at), "address");
  CHECK_EQUAL(fmt::format("{}", address_type{}), "address");
  CHECK(!caf::holds_alternative<address_type>(t));
  CHECK(caf::holds_alternative<address_type>(at));
  const auto lat = type{legacy_address_type{}};
  CHECK(caf::holds_alternative<address_type>(lat));
}

TEST(subnet_type) {
  static_assert(concrete_type<subnet_type>);
  static_assert(basic_type<subnet_type>);
  static_assert(!complex_type<subnet_type>);
  const auto t = type{};
  const auto st = type{subnet_type{}};
  CHECK(st);
  CHECK_EQUAL(as_bytes(st), as_bytes(subnet_type{}));
  CHECK(t != st);
  CHECK(t < st);
  CHECK(t <= st);
  CHECK_EQUAL(fmt::format("{}", st), "subnet");
  CHECK_EQUAL(fmt::format("{}", subnet_type{}), "subnet");
  CHECK(!caf::holds_alternative<subnet_type>(t));
  CHECK(caf::holds_alternative<subnet_type>(st));
  const auto lst = type{legacy_subnet_type{}};
  CHECK(caf::holds_alternative<subnet_type>(lst));
}

TEST(enumeration_type) {
  static_assert(concrete_type<enumeration_type>);
  static_assert(!basic_type<enumeration_type>);
  static_assert(complex_type<enumeration_type>);
  const auto t = type{};
  const auto et = type{enumeration_type{{{"first"}, {"third", 2}, {"fourth"}}}};
  CHECK(et);
  CHECK(t != et);
  CHECK(t < et);
  CHECK(t <= et);
  CHECK_EQUAL(fmt::format("{}", et), "enum {first: 0, third: 2, fourth: 3}");
  CHECK(!caf::holds_alternative<enumeration_type>(t));
  CHECK(caf::holds_alternative<enumeration_type>(et));
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(0), "first");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(1), "");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(2), "third");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(3), "fourth");
  const auto let = type{legacy_enumeration_type{{"first", "second", "third"}}};
  CHECK(caf::holds_alternative<enumeration_type>(let));
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(0), "first");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(1), "second");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(2), "third");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(3), "");
}

TEST(list_type) {
  static_assert(concrete_type<list_type>);
  static_assert(!basic_type<list_type>);
  static_assert(complex_type<list_type>);
  const auto t = type{};
  const auto lit = type{list_type{integer_type{}}};
  CHECK(lit);
  CHECK_EQUAL(as_bytes(lit), as_bytes(list_type{integer_type{}}));
  CHECK(t != lit);
  CHECK(t < lit);
  CHECK(t <= lit);
  CHECK_EQUAL(fmt::format("{}", lit), "list<integer>");
  CHECK_EQUAL(fmt::format("{}", list_type{{}}), "list<none>");
  CHECK(!caf::holds_alternative<list_type>(t));
  CHECK(caf::holds_alternative<list_type>(lit));
  CHECK_EQUAL(caf::get<list_type>(lit).value_type(), type{integer_type{}});
  const auto llbt = type{legacy_list_type{legacy_bool_type{}}};
  CHECK(caf::holds_alternative<list_type>(llbt));
  CHECK_EQUAL(caf::get<list_type>(llbt).value_type(), type{bool_type{}});
}

TEST(map_type) {
  static_assert(concrete_type<map_type>);
  static_assert(!basic_type<map_type>);
  static_assert(complex_type<map_type>);
  const auto t = type{};
  const auto msit = type{map_type{string_type{}, integer_type{}}};
  CHECK(msit);
  CHECK_EQUAL(as_bytes(msit),
              as_bytes(map_type{string_type{}, integer_type{}}));
  CHECK(t != msit);
  CHECK(t < msit);
  CHECK(t <= msit);
  CHECK_EQUAL(fmt::format("{}", msit), "map<string, integer>");
  CHECK_EQUAL(fmt::format("{}", map_type{{}, {}}), "map<none, none>");
  CHECK(!caf::holds_alternative<map_type>(t));
  CHECK(caf::holds_alternative<map_type>(msit));
  CHECK_EQUAL(caf::get<map_type>(msit).key_type(), type{string_type{}});
  CHECK_EQUAL(caf::get<map_type>(msit).value_type(), type{integer_type{}});
  const auto lmabt
    = type{legacy_map_type{legacy_address_type{}, legacy_bool_type{}}};
  CHECK(caf::holds_alternative<map_type>(lmabt));
  CHECK_EQUAL(caf::get<map_type>(lmabt).key_type(), type{address_type{}});
  CHECK_EQUAL(caf::get<map_type>(lmabt).value_type(), type{bool_type{}});
}

TEST(record_type) {
  static_assert(concrete_type<record_type>);
  static_assert(!basic_type<record_type>);
  static_assert(complex_type<record_type>);
  const auto t = type{};
  const auto rt = type{record_type{
    {"i", integer_type{}},
    {"r1",
     record_type{
       {"p", type{"port", integer_type{}}},
       {"a", address_type{}},
     }},
    {"b", bool_type{}},
    {"r2",
     record_type{
       {"s", subnet_type{}},
     }},
  }};
  CHECK_EQUAL(fmt::format("{}", rt), "record {i: integer, r1: record {p: port, "
                                     "a: address}, b: bool, r2: record {s: "
                                     "subnet}}");
  const auto& r = caf::get<record_type>(rt);
  CHECK_EQUAL(r.field(2).type, bool_type{});
  CHECK_EQUAL(r.field({1, 1}).type, address_type{});
  CHECK_EQUAL(r.field({3, 0}).name, "s");
  CHECK_EQUAL(fmt::format("{}", fmt::join(r.fields(), ", ")),
              "i: integer, r1: record {p: port, a: address}, b: bool, r2: "
              "record {s: subnet}");
  CHECK_EQUAL(fmt::format("{}", fmt::join(flatten(r).fields(), ", ")),
              "i: integer, p: port, a: address, b: bool, s: subnet");
  CHECK_EQUAL(flatten(rt), type{flatten(r)});
}

TEST(legacy_type conversion) {
  const auto rt = type{record_type{
    {"i", integer_type{}},
    {"r1",
     record_type{
       {"p", type{"port", integer_type{}}},
       {"a", address_type{}},
     }},
    {"b", type{bool_type{}, {{"key"}}}},
    {"r2",
     record_type{
       {"s", type{subnet_type{}, {{"key", "value"}}}},
     }},
  }};
  const auto lrt = legacy_type{legacy_record_type{
    {"i", legacy_integer_type{}},
    {"r1",
     legacy_record_type{
       {"p", legacy_alias_type{legacy_integer_type{}}.name("port")},
       {"a", legacy_address_type{}},
     }},
    {"b", legacy_bool_type{}.attributes({{"key"}})},
    {"r2",
     legacy_record_type{
       {"s", legacy_subnet_type{}.attributes({{"key", "value"}})},
     }},
  }};
  // Note that rt == type{lrt} fails because the types are semantically
  // equivalent, but not exactly equivalent because of the inconsistent handling
  // of naming in legacy types. As such, the following checks fail:
  //   CHECK_EQUAL(rt, type{lrt});
  //   CHECK_EQUAL(legacy_type{rt}, lrt);
  // Instead, we instead compare the printed versions of the types for
  // equivalence.
  CHECK_EQUAL(fmt::format("{}", rt), fmt::format("{}", type{lrt}));
  CHECK_EQUAL(fmt::format("{}", legacy_type{rt}), fmt::format("{}", lrt));
}

TEST(named types) {
  const auto at = type{"l1", bool_type{}};
  CHECK(caf::holds_alternative<bool_type>(at));
  CHECK_EQUAL(at.name(), "l1");
  CHECK_EQUAL(fmt::format("{}", at), "l1");
  const auto aat = type{"l2", at};
  CHECK(caf::holds_alternative<bool_type>(aat));
  CHECK_EQUAL(aat.name(), "l2");
  CHECK_EQUAL(fmt::format("{}", aat), "l2");
  const auto lat = type{legacy_bool_type{}.name("l3")};
  CHECK(caf::holds_alternative<bool_type>(lat));
  CHECK_EQUAL(lat.name(), "l3");
  CHECK_EQUAL(fmt::format("{}", lat), "l3");
}

TEST(tagged types) {
  const auto at = type{bool_type{}, {{"first", "value"}, {"second"}}};
  CHECK(caf::holds_alternative<bool_type>(at));
  CHECK_EQUAL(at.name(), "");
  CHECK_EQUAL(at.tag("first"), "value");
  CHECK_EQUAL(at.tag("second"), "");
  CHECK_EQUAL(at.tag("third"), std::nullopt);
  CHECK_EQUAL(at.tag("fourth"), std::nullopt);
  CHECK_EQUAL(fmt::format("{}", at), "bool #first=value #second");
  const auto aat = type{"l2", at, {{"third", "nestingworks"}}};
  CHECK(caf::holds_alternative<bool_type>(aat));
  CHECK_EQUAL(aat.name(), "l2");
  CHECK_EQUAL(aat.tag("first"), "value");
  CHECK_EQUAL(aat.tag("second"), "");
  CHECK_EQUAL(aat.tag("third"), "nestingworks");
  CHECK_EQUAL(aat.tag("fourth"), std::nullopt);
  CHECK_EQUAL(fmt::format("{}", aat), "l2 #third=nestingworks #first=value "
                                      "#second");
  const auto lat
    = type{legacy_bool_type{}.attributes({{"first", "value"}, {"second"}})};
  CHECK_EQUAL(lat, at);
}

TEST(sorting) {
  auto ts = std::vector<type>{
    none_type{},
    bool_type{},
    integer_type{},
    type{"custom_none", none_type{}},
    type{"custom_bool", bool_type{}},
    type{"custom_integer", integer_type{}},
  };
  std::shuffle(ts.begin(), ts.end(), std::random_device());
  std::sort(ts.begin(), ts.end());
  const char* expected
    = "none bool integer custom_bool custom_none custom_integer";
  CHECK_EQUAL(fmt::format("{}", fmt::join(ts, " ")), expected);
}

TEST(sum type) {
  // Returns a visitor that checks whether the expected concrete types are the
  // types resulting in the visitation.
  auto is_type = []<concrete_type... T>(const T&...) {
    return []<concrete_type... U>(const U&...) {
      return (std::is_same_v<T, U> && ...);
    };
  };
  CHECK(caf::visit(is_type(none_type{}), type{}));
  CHECK(caf::visit(is_type(none_type{}), type{none_type{}}));
  CHECK(caf::visit(is_type(bool_type{}), type{bool_type{}}));
  CHECK(caf::visit(is_type(bool_type{}, integer_type{}), type{bool_type{}},
                   type{integer_type{}}));
}

TEST(hashes) {
  auto hash = []<type_or_concrete_type T>(const T& value) {
    auto hasher = std::hash<T>{};
    return hasher(value);
  };
  CHECK_EQUAL(hash(none_type{}), 0x5DF28E92BCCA4531ul);
  CHECK_EQUAL(hash(bool_type{}), 0xBFF0C79D40554449ul);
  CHECK_EQUAL(hash(integer_type{}), 0xD8C66D08F868662Bul);
  CHECK_EQUAL(hash(count_type{}), 0x2F80823CB9D60C3Bul);
  CHECK_EQUAL(hash(real_type{}), 0x8AC3473B0C9FDB7Aul);
  CHECK_EQUAL(hash(duration_type{}), 0x9FB2CA5D9CDF512Aul);
  CHECK_EQUAL(hash(time_type{}), 0x379DC79C15D4FC1Aul);
  CHECK_EQUAL(hash(string_type{}), 0x3F92527B5CA01E46ul);
  CHECK_EQUAL(hash(pattern_type{}), 0xB58A4DFCBCAB3AA0ul);
  CHECK_EQUAL(hash(address_type{}), 0xB195BC7644771465ul);
  CHECK_EQUAL(hash(subnet_type{}), 0xCF652DBCCA4AAED5ul);
  CHECK_EQUAL(hash(enumeration_type{{{"a"}, {"b"}, {"c"}}}),
              0x624171C602B39999ul);
  CHECK_EQUAL(hash(list_type{integer_type{}}), 0xFAE238FED25FDCD0ul);
  CHECK_EQUAL(hash(map_type{time_type{}, string_type{}}), 0xF6694A1437D5D288ul);
  CHECK_EQUAL(hash(record_type{{"a", address_type{}}, {"b", bool_type{}}}),
              0x4BB2B1174A8B3788ul);
}

TEST(congruence) {
  auto i = type{integer_type{}};
  auto j = type{integer_type{}};
  CHECK(i == j);
  i = type{"i", i};
  j = type{"j", j};
  CHECK(i != j);
  auto c = type{"c", count_type{}};
  CHECK(congruent(i, i));
  CHECK(congruent(i, j));
  CHECK(!congruent(i, c));
  auto l0 = type{list_type{i}};
  auto l1 = type{list_type{j}};
  auto l2 = type{list_type{c}};
  CHECK(l0 != l1);
  CHECK(l0 != l2);
  CHECK(congruent(l0, l1));
  CHECK(!congruent(l1, l2));
  auto r0 = type{record_type{
    {"a", address_type{}},
    {"b", bool_type{}},
    {"c", count_type{}},
  }};
  auto r1 = type{record_type{
    {"x", address_type{}},
    {"y", bool_type{}},
    {"z", count_type{}},
  }};
  CHECK(r0 != r1);
  CHECK(congruent(r0, r1));
  auto a = type{"a", i};
  CHECK(a != i);
  CHECK(congruent(a, i));
  a = type{"r0", r0};
  CHECK(a != r0);
  CHECK(congruent(a, r0));
  CHECK(congruent(type{}, type{}));
  CHECK(!congruent(type{string_type{}}, type{}));
  CHECK(!congruent(type{}, type{string_type{}}));
}

TEST(compatibility) {
  CHECK(compatible(type{address_type{}}, relational_operator::in,
                   type{subnet_type{}}));
  CHECK(compatible(type{address_type{}}, relational_operator::in, subnet{}));
  CHECK(compatible(type{subnet_type{}}, relational_operator::in,
                   type{subnet_type{}}));
  CHECK(compatible(type{subnet_type{}}, relational_operator::in, subnet{}));
}

FIXTURE_SCOPE(type_fixture, fixtures::deterministic_actor_system)

TEST(serialization) {
  CHECK_ROUNDTRIP(type{});
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
  CHECK_ROUNDTRIP(type{enumeration_type{{{"a"}, {"b"}, {"c"}}}});
  CHECK_ROUNDTRIP(type{list_type{integer_type{}}});
  CHECK_ROUNDTRIP(type{map_type{address_type{}, subnet_type{}}});
  const auto rt = type{record_type{
    {"i", integer_type{}},
    {"r1",
     record_type{
       {"p", type{"port", integer_type{}}},
       {"a", address_type{}},
     }},
    {"b", bool_type{}},
    {"r2",
     record_type{
       {"s", subnet_type{}},
     }},
  }};
  CHECK_ROUNDTRIP(rt);
}

FIXTURE_SCOPE_END()

} // namespace vast
