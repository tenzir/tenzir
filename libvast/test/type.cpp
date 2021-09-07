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

} // namespace vast
