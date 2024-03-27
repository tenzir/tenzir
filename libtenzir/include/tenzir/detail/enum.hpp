//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/inspect_enum_str.hpp"
#include "tenzir/detail/pp.hpp"
#include "tenzir/tag.hpp"

#include <fmt/format.h>

#define TENZIR_ENUM_STR(x) #x,
#define TENZIR_ENUM_CHECK(x)                                                   \
  if (str == #x) {                                                             \
    return x;                                                                  \
  }
#define TENZIR_ENUM_CASE(x)                                                    \
  case x:                                                                      \
    return #x;

/// Defines an `enum class` with implementations for `inspect` (that uses
/// strings if the inspector has a human-readable format), `to_string`,
/// `from_string`, and `fmt::formatter` (that uses the same flags as
/// `std::string_view`).
#define TENZIR_ENUM(name, ...)                                                 \
  enum class name { __VA_ARGS__ };                                             \
  [[maybe_unused]] inline auto to_string(name x) -> std::string_view {         \
    switch (x) {                                                               \
      using enum name;                                                         \
      TENZIR_PP_FOR(TENZIR_ENUM_CASE, __VA_ARGS__)                             \
    }                                                                          \
    TENZIR_UNREACHABLE();                                                      \
  }                                                                            \
  [[maybe_unused]] inline auto adl_from_string(                                \
    tenzir::tag<name>, std::string_view str) -> std::optional<name> {          \
    using enum name;                                                           \
    TENZIR_PP_FOR(TENZIR_ENUM_CHECK, __VA_ARGS__);                             \
    return std::nullopt;                                                       \
  }                                                                            \
  [[maybe_unused]] auto inspect(auto& f, name& x) -> bool {                    \
    return tenzir::detail::inspect_enum_str(                                   \
      f, x, {TENZIR_PP_FOR(TENZIR_ENUM_STR, __VA_ARGS__)});                    \
  }                                                                            \
  [[maybe_unused]] void adl_tenzir_macro_enum(name)

namespace tenzir {

template <class T>
  requires requires { adl_from_string(tag_v<T>, std::string_view{}); }
auto from_string(std::string_view str) -> std::optional<T> {
  return adl_from_string(tag_v<T>, str);
}

} // namespace tenzir

template <class T>
  requires(std::is_same_v<decltype(adl_tenzir_macro_enum(T{})), void>)
struct fmt::formatter<T> : formatter<std::string_view> {
  template <class FormatContext>
  auto format(const T& x, FormatContext& ctx) const {
    return formatter<std::string_view>::format(to_string(x), ctx);
  }
};
