#ifndef VAST_CONCEPT_PARSEABLE_CORE_NOT_H
#define VAST_CONCEPT_PARSEABLE_CORE_NOT_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

template <typename Parser>
class not_parser : public parser<not_parser<Parser>>
{
public:
  using attribute = typename Parser::attribute;

  not_parser(Parser const& p)
    : parser_{p}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    // Do not consume input.
    auto i = f;
    return ! parser_.parse(i, l, a);
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
