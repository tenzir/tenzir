#ifndef VAST_CONCEPT_PARSEABLE_VAST_KEY_H
#define VAST_CONCEPT_PARSEABLE_VAST_KEY_H

#include "vast/key.h"

#include "vast/concept/parseable/core/list.h"
#include "vast/concept/parseable/core/operators.h"
#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/core/plus.h"
#include "vast/concept/parseable/string/char_class.h"

namespace vast {

struct key_parser : parser<key_parser> {
  using attribute = key;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    using namespace parsers;
    static auto p = +(alnum | chr{'_'} | chr{':'}) % '.';
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<key> {
  using type = key_parser;
};

namespace parsers {

static auto const key = make_parser<vast::key>();

} // namespace parsers

} // namespace vast

#endif
