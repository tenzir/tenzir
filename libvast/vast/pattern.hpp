#ifndef VAST_PATTERN_HPP
#define VAST_PATTERN_HPP

#include <string>

#include "vast/detail/operators.hpp"

namespace vast {

struct access;
class json;

/// A regular expression.
class pattern : detail::totally_ordered<pattern> {
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
  static pattern glob(std::string const& str);

  /// Default-constructs an empty pattern.
  pattern() = default;

  /// Constructs a pattern from a string.
  /// @param str The string containing the pattern.
  explicit pattern(std::string str);

  /// Matches a string against the pattern.
  /// @param str The string to match.
  /// @returns `true` if the pattern matches exactly *str*.
  bool match(std::string const& str) const;

  /// Searches a pattern in a string.
  /// @param str The string to search.
  /// @returns `true` if the pattern matches inside *str*.
  bool search(std::string const& str) const;

  const std::string& string() const;

  friend bool operator==(pattern const& lhs, pattern const& rhs);
  friend bool operator<(pattern const& lhs, pattern const& rhs);

  template <class Inspector>
  friend auto inspect(Inspector& f, pattern& p) {
    return f(p.str_);
  }

  friend bool convert(pattern const& p, json& j);

private:
  std::string str_;
};

} // namespace vast

#endif
