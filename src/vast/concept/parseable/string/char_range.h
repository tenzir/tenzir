#ifndef VAST_CONCEPT_PARSEABLE_STRING_CHAR_RANGE_H
#define VAST_CONCEPT_PARSEABLE_STRING_CHAR_RANGE_H

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/detail/char_helpers.h"

namespace vast {

/// Parses a single ASCII character in a given continguous range.
template <char From, char To, char... Ranges>
class char_range_parser : public parser<char_range_parser<From, To>>
{
public:
  using attribute = char;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    if (f == l || ! check<From, To, Ranges...>(*f))
      return false;
    detail::absorb(a, *f);
    ++f;
    return true;
  }

private:
  template <char L, char R>
  static bool check(char c)
  {
    return L <= c && c <= R;
  }

  template <char L0, char R0, char L1, char R1, char... Cs>
  static bool check(char c)
  {
    return check<L0, R0>(c) && check<L1, R1, Cs...>;
  }
};

} // namespace vast

#endif

