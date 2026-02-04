//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string_view>
#include <utility>

namespace tenzir {

/// The return type of `std::forward_like<T>(u)`.
template <class T, class U>
using ForwardLike = decltype(std::forward_like<T>(std::declval<U>()));

/// Returns a human-readable name for type `T`.
template <typename T>
consteval auto type_name() -> std::string_view {
#if defined _WIN32
  constexpr std::string_view s = __FUNCTION__;
  const auto begin_search = s.find_first_of("<");
  const auto space = s.find(' ', begin_search);
  const auto begin_type = space != s.npos ? space + 1 : begin_search + 1;
  const auto end_type = s.find_last_of(">");
  return s.substr(begin_type, end_type - begin_type);
#elif defined __GNUC__
  constexpr std::string_view s = __PRETTY_FUNCTION__;
  constexpr std::string_view t_equals = "T = ";
  const auto begin_type = s.find(t_equals) + t_equals.size();
  const auto end_type = s.find_first_of(";]", begin_type);
  return s.substr(begin_type, end_type - begin_type);
#endif
}

} // namespace tenzir
