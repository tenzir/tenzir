#ifndef VAST_CONCEPT_PARSEABLE_CORE_PLUS_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_PLUS_HPP

#include <vector>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/container.hpp"

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
