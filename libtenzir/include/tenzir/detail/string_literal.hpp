//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <algorithm>
#include <cstdint>
#include <string_view>

namespace tenzir::detail {

/// A string literal wrapper for NTTPs, making it possible to use string
/// literals as template arguments, which would not normally be possible.
template <size_t N>
  requires(N >= 1)
struct string_literal {
  constexpr explicit(false)
    string_literal(const char (&str)[N]) noexcept { // NOLINT
    std::copy_n(str, N, value);
  }

  char value[N] = {}; // NOLINT

  [[nodiscard]] constexpr auto str() const noexcept -> std::string_view {
    return {&value[0], &value[N - 1]};
  }

  [[nodiscard]] explicit(false) constexpr
  operator std::string_view() const noexcept {
    return str();
  }

  [[nodiscard]] explicit(false) constexpr
  operator std::string() const noexcept {
    return std::string{str()};
  }
};
} // namespace tenzir::detail

template <size_t N>
struct fmt::formatter<tenzir::detail::string_literal<N>>
  : fmt::formatter<std::string_view> {
  auto
  format(tenzir::detail::string_literal<N> s, fmt::format_context& ctx) const {
    return fmt::formatter<std::string_view>::format(s.str(), ctx);
  }
};
