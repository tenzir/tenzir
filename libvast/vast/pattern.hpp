//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/operators.hpp"

#include <fmt/format.h>

#include <string>

namespace vast {

struct access;
class data;

/// A regular expression.
class pattern : detail::totally_ordered<pattern>,
                detail::addable<pattern>,
                detail::orable<pattern>,
                detail::andable<pattern> {
  friend access;

public:
  /// Constructs a pattern from a glob expression. A glob expression consists
  /// of the following elements:
  ///
  ///     - `*`   Equivalent to `.*` in a regex
  ///     - `?`    Equivalent to `.` in a regex
  ///     - `[ab]` Equivalent to the character class `[ab]` in a regex.
  ///
  /// @param str The glob expression.
  /// @returns A pattern for the glob expression *str*.
  static pattern glob(std::string_view str);

  /// Default-constructs an empty pattern.
  pattern() = default;

  /// Constructs a pattern from a string.
  /// @param str The string containing the pattern.
  explicit pattern(std::string str);

  /// Matches a string against the pattern.
  /// @param str The string to match.
  /// @returns `true` if the pattern matches exactly *str*.
  bool match(std::string_view str) const;

  /// Searches a pattern in a string.
  /// @param str The string to search.
  /// @returns `true` if the pattern matches inside *str*.
  bool search(std::string_view str) const;

  const std::string& string() const;

  // -- concepts // ------------------------------------------------------------

  pattern& operator+=(const pattern& other);
  pattern& operator+=(std::string_view other);
  pattern& operator|=(const pattern& other);
  pattern& operator|=(std::string_view other);
  pattern& operator&=(const pattern& other);
  pattern& operator&=(std::string_view other);

  friend pattern operator+(const pattern& x, std::string_view y);
  friend pattern operator+(std::string_view x, const pattern& y);
  friend pattern operator|(const pattern& x, std::string_view y);
  friend pattern operator|(std::string_view x, const pattern& y);
  friend pattern operator&(const pattern& x, std::string_view y);
  friend pattern operator&(std::string_view x, const pattern& y);

  friend bool operator==(const pattern& lhs, const pattern& rhs);
  friend bool operator<(const pattern& lhs, const pattern& rhs);

  // We are not using detail::equality_comparable here because it takes both
  // arguments by const reference. Here, string_view is taken by value.
  friend bool operator==(const pattern& lhs, std::string_view rhs);
  friend bool operator!=(const pattern& lhs, std::string_view rhs);
  friend bool operator==(std::string_view lhs, const pattern& rhs);
  friend bool operator!=(std::string_view lhs, const pattern& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, pattern& p) {
    return f(p.str_);
  }

  friend bool convert(const pattern& p, data& d);

private:
  std::string str_;
};

} // namespace vast

namespace fmt {
/// Custom formatter for `vast::pattern` type.
template <>
struct formatter<vast::pattern> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return std::end(ctx);
  }

  template <class P, class FormatContext>
  auto format(const P& p, FormatContext& ctx) {
    return format_to(ctx.out(), "/{}/", p.string());
  }
};

} // namespace fmt
