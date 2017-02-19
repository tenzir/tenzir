#ifndef VAST_CONCEPT_PARSEABLE_CORE_GUARD_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_GUARD_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Attaches a guard expression to a parser that must succeed after the parser
/// executes.
/// @tparam Parser The parser to augment with a guard expression.
/// @tparam Guard A function that takes the synthesized attribute by
///               const-reference and returns `bool`.
template <typename Parser, typename Guard>
class guard_parser : public parser<guard_parser<Parser, Guard>> {
public:
  using attribute = typename Parser::attribute;

  guard_parser(Parser p, Guard fun) : parser_{std::move(p)}, guard_(fun) {
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    return parser_(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    attribute attr;
    if (!(parser_(f, l, attr) && guard_(attr)))
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
