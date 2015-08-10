#ifndef VAST_CONCEPT_PARSEABLE_CORE_PLUS_H
#define VAST_CONCEPT_PARSEABLE_CORE_PLUS_H

#include <vector>

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/detail/container.h"

namespace vast {

template <typename Parser>
class plus_parser : public parser<plus_parser<Parser>> {
public:
  using container = detail::container<typename Parser::attribute>;
  using attribute = typename container::attribute;

  explicit plus_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    if (!container::parse(parser_, f, l, a))
      return false;
    while (container::parse(parser_, f, l, a))
      ;
    return true;
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
