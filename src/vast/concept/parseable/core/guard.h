#ifndef VAST_CONCEPT_PARSEABLE_CORE_GUARD_H
#define VAST_CONCEPT_PARSEABLE_CORE_GUARD_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Attaches a guard expression to a parser that must succeed in adition to the
/// parser itself.
/// @tparam Parser The parser to augment with a guard expression.
/// @tparam Guard The guard function which simply take the synthesized
///               attribute by const-reference and returns `bool`.
template <typename Parser, typename Guard>
class guard_parser : public parser<guard_parser<Parser, Guard>>
{
public:
  // We keep the semantic action transparent and just haul through the parser's
  // attribute.
  using attribute = typename Parser::attribute;

  guard_parser(Parser const& p, Guard fun)
    : parser_{p},
      guard_(fun)
  {
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    return parser_.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    attribute attr;
    if (! (parser_.parse(f, l, attr) && guard_(attr)))
      return false;
    a = std::move(attr);
    return true;
  }

private:
  Parser parser_;
  Guard guard_;
};

} // namespace vast

#endif
