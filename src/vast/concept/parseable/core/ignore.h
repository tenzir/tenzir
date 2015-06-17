#ifndef VAST_CONCEPT_PARSEABLE_CORE_IGNORE_H
#define VAST_CONCEPT_PARSEABLE_CORE_IGNORE_H

#include "vast/concept/parseable/core/parser.h"

namespace vast {

/// Wraps a parser and ignores its attribute.
template <typename Parser>
class ignore_parser : public parser<ignore_parser<Parser>>
{
public:
  using attribute = unused_type;

  ignore_parser(Parser const& p)
    : parser_{p}
  {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute&) const
  {
    return parser_.parse(f, l, unused);
  }

private:
  Parser parser_;
};

template <typename Parser>
ignore_parser<Parser> ignore(Parser const& p)
{
  return p;
}

} // namespace vast

#endif
