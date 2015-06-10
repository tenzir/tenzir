#ifndef VAST_CONCEPT_PARSEABLE_CORE_ACTION_H
#define VAST_CONCEPT_PARSEABLE_CORE_ACTION_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Transform a parser's inner attribute after a successful parse.
/// @tparam Parser The parser to augment with an action.
/// @tparam Action A function taking the synthesized attribute and returning
///                a new type.
template <typename Parser, typename Action>
class action_parser : public parser<action_parser<Parser, Action>>
{
public:
  using inner_attribute = typename Parser::attribute;
  using attribute = std::result_of_t<Action(inner_attribute)>;

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
    inner_attribute x;
    if (! parser_.parse(f, l, x))
      return false;
    a = action_(std::move(x));
    return true;
  }

private:
  Parser parser_;
  Action action_;
};

} // namespace vast

#endif
