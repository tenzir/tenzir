#ifndef VAST_CONCEPT_PARSEABLE_VAST_PATTERN_H
#define VAST_CONCEPT_PARSEABLE_VAST_PATTERN_H

#include "vast/pattern.h"

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string/quoted_string.h"

namespace vast {

using pattern_parser = quoted_string_parser<'/', '\\'>;

template <>
struct access::parser<pattern> : vast::parser<access::parser<pattern>>
{
  using attribute = pattern;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    static auto const p = pattern_parser{};
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, pattern& a) const
  {
    static auto const p = pattern_parser{};
    return p.parse(f, l, a.str_);
  }
};

template <>
struct parser_registry<pattern>
{
  using type = access::parser<pattern>;
};

namespace parsers {

static auto const pattern = make_parser<vast::pattern>();

} // namespace parsers

} // namespace vast

#endif
