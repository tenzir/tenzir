#ifndef VAST_CONCEPT_PARSEABLE_CORE_ACTION_H
#define VAST_CONCEPT_PARSEABLE_CORE_ACTION_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Invokes an action after a successful parse.
/// @tparam Parser The parser to augment with an action.
/// @tparam Action A function taking the synthesized attribute and returning
///                `void`.
template <typename Parser, typename Action>
class action_parser : public parser<action_parser<Parser, Action>>
{
public:
  // We keep the semantic action transparent and just haul through the parser's
  // attribute.
  using attribute = typename Parser::attribute;

  action_parser(Parser const& p, Action fun)
    : parser_{p},
      action_(fun)
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
    if (! parser_.parse(f, l, a))
      return false;
    action_(a);
    return true;
  }

private:
  Parser parser_;
  Action action_;
};

} // namespace vast

#endif
