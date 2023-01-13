//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <cstdint>
#include <string_view>

namespace vast::detail {

/// String literal wrapper for NTTPs.
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
};

} // namespace vast::detail
