#ifndef VAST_CONCEPT_PARSEABLE_VAST_OFFSET_H
#define VAST_CONCEPT_PARSEABLE_VAST_OFFSET_H

#include "vast/offset.h"

#include "vast/concept/parseable/core/list.h"
#include "vast/concept/parseable/core/operators.h"
#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/numeric/integral.h"

namespace vast {

struct offset_parser : parser<offset_parser>
{
  using attribute = offset;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    static auto p = parsers::u32 % ',';
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<offset>
{
  using type = offset_parser;
};

namespace parsers {

static auto const offset = make_parser<vast::offset>();

} // namespace parsers

} // namespace vast

#endif
