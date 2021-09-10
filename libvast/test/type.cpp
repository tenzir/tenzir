//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type

#include "vast/type.hpp"

#include "vast/detail/overload.hpp"
#include "vast/legacy_type.hpp"
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
  CHECK_EQUAL(fmt::format("{}", lit), "list");
  CHECK_EQUAL(fmt::format("{}", list_type{{}}), "list");
  CHECK(!caf::holds_alternative<list_type>(t));
  CHECK(caf::holds_alternative<list_type>(lit));
  CHECK_EQUAL(caf::get<list_type>(lit).value_type(), type{integer_type{}});
  const auto llbt = type{legacy_list_type{legacy_bool_type{}}};
  CHECK(caf::holds_alternative<list_type>(llbt));
  CHECK_EQUAL(caf::get<list_type>(llbt).value_type(), type{bool_type{}});
}

TEST(alias_type) {
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
    = "none bool integer custom_none custom_bool custom_integer";
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

} // namespace vast
