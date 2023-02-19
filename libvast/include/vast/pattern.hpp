//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>

#include <regex>
#include <string>

namespace vast {

struct access;
class data;

/// A regular expression.
class pattern {
  friend access;

public:
  /// optional flag to make a pattern string case-insensitive.
  static inline auto constexpr case_insensitive_flag = 'i';

  /// Default-constructs an empty pattern.
  pattern() = default;

  static auto make(std::string str, bool case_insensitive = false) noexcept
    -> caf::expected<pattern>;

  /// Matches a string against the pattern.
  /// @param str The string to match.
  /// @returns `true` if the pattern matches exactly *str*.
  [[nodiscard]] bool match(std::string_view str) const;

  /// Searches a pattern in a string.
  /// @param str The string to search.
  /// @returns `true` if the pattern matches inside *str*.
  [[nodiscard]] bool search(std::string_view str) const;

  [[nodiscard]] const std::string& string() const;

  [[nodiscard]] bool case_insensitive() const;

  // -- concepts // ------------------------------------------------------------

  friend bool operator==(const pattern& lhs, const pattern& rhs) noexcept;
  friend std::strong_ordering
  operator<=>(const pattern& lhs, const pattern& rhs) noexcept;

  friend bool operator==(const pattern& lhs, std::string_view rhs) noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, pattern& p) -> bool {
    auto ok = detail::apply_all(f, p.str_, p.case_insensitive_);
    if constexpr (Inspector::is_loading) {
      auto result = pattern::make(std::move(p.str_), p.case_insensitive_);
      VAST_ASSERT(result, fmt::to_string(result.error()).c_str());
      p = std::move(*result);
    }
    return ok;
  }

  friend bool convert(const pattern& p, data& d);

private:
  /// Constructs a pattern from a string.
  /// @param str The string containing the pattern.
  explicit pattern(std::string str, bool case_insensitive, std::regex regex);

  std::string str_ = {};
  bool case_insensitive_ = {};
  std::regex regex_ = {};
};

} // namespace vast
