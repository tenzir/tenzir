//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE type_traits

#include "vast/detail/type_traits.hpp"

#include "vast/test/test.hpp"

#include <caf/variant.hpp>

#include <variant>

namespace {
template <class... Ts>
struct fake_list {};

template <template <class...> class TList>
constexpr auto check() {
  static_assert(vast::detail::contains_type_v<TList<int, double>, double>);
  static_assert(not vast::detail::contains_type_v<TList<int, double>, char>);
}
} // namespace

TEST(contains_type) {
  check<std::variant>();
  check<std::tuple>();
  check<fake_list>();
  check<caf::variant>();
}
