#ifndef VAST_CONCEPT_PARSEABLE_STRING_ANY_H
#define VAST_CONCEPT_PARSEABLE_STRING_ANY_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

struct any_parser : public parser<any_parser> {
  using attribute = char;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    if (f == l)
      return false;
    detail::absorb(a, *f);
    ++f;
    return true;
  }
};

template <>
struct parser_registry<char> {
  using type = any_parser;
};

namespace parsers {

static auto const any = any_parser{};

} // namespace parsers
} // namespace vast

#endif
