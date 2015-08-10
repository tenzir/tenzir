#ifndef VAST_CONCEPT_PARSEABLE_STRING_CHAR_H
#define VAST_CONCEPT_PARSEABLE_STRING_CHAR_H

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/detail/char_helpers.h"

namespace vast {

/// Parses a specific character.
class char_parser : public parser<char_parser> {
public:
  using attribute = char;

  char_parser(char c) : c_{c} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    if (f == l || *f != c_)
      return false;
    detail::absorb(a, c_);
    ++f;
    return true;
  }

private:
  char c_;
};

namespace parsers {

using chr = char_parser;

} // namespace parsers
} // namespace vast

#endif
