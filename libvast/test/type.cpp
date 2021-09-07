//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type

#include "vast/type.hpp"

#include "vast/legacy_type.hpp"
#include "vast/test/test.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <random>

namespace vast {

TEST(none_type) {
  static_assert(concrete_type<none_type>);
  static_assert(basic_type<none_type>);
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

} // namespace vast
