#ifndef VAST_PATTERN_H
#define VAST_PATTERN_H

#include <string>
#include "vast/parse.h"
#include "vast/print.h"
#include "vast/util/operators.h"

namespace vast {

/// A regular expression.
class pattern : util::totally_ordered<pattern>
{
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

private:
  std::string str_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(pattern const& p, Iterator&& out)
  {
    *out++ = '/';

    auto t = print(p.str_, out);
    if (! t)
      return t.error();

    *out++ = '/';

    return nothing;
  }

  template <typename Iterator>
  friend trial<void> parse(pattern& p, Iterator& begin, Iterator end)
  {
    if (*begin != '/')
      return error{"pattern did not begin with a '/'"};

    auto t = parse<std::string>(begin, end);
    if (! t)
      return t.error();

    if (t->empty() || (*t)[t->size() - 1] != '/')
      return error{"pattern did not end with a '/'"};

    p = pattern{t->substr(1, t->size() - 2)};

    begin = end;
    return nothing;
  }

  friend bool operator==(pattern const& lhs, pattern const& rhs);
  friend bool operator<(pattern const& lhs, pattern const& rhs);
};

trial<void> convert(pattern const& p, util::json& j);

} // namespace vast

#endif
