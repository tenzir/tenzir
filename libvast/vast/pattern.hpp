/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <string>

#include "vast/detail/operators.hpp"

namespace vast {

struct access;
class json;

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

  template <class Inspector>
  friend auto inspect(Inspector& f, pattern& p) {
    return f(p.str_);
  }

  friend bool convert(const pattern& p, json& j);

private:
  std::string str_;
};

} // namespace vast

