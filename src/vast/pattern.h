#ifndef VAST_PATTERN_H
#define VAST_PATTERN_H

#include <string>

#include "vast/util/operators.h"

namespace vast {

struct access;

/// A regular expression.
class pattern : util::totally_ordered<pattern> {
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

  friend bool operator==(pattern const& lhs, pattern const& rhs);
  friend bool operator<(pattern const& lhs, pattern const& rhs);

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

private:
  std::string str_;
};

} // namespace vast

#endif
