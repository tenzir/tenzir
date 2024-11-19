//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/type_traits.hpp"

#include "tenzir/test/test.hpp"

#include <variant>

namespace {
template <class... Ts>
struct fake_list {};

template <template <class...> class TList>
constexpr auto check() {
  static_assert(tenzir::detail::contains_type_v<TList<int, double>, double>);
  static_assert(not tenzir::detail::contains_type_v<TList<int, double>, char>);
}
} // namespace

TEST(contains_type) {
  check<std::variant>();
  check<std::tuple>();
  check<fake_list>();
  check<caf::variant>();
}
