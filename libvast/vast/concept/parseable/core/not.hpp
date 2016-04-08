#ifndef VAST_CONCEPT_PARSEABLE_CORE_NOT_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_NOT_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <typename Parser>
class not_parser : public parser<not_parser<Parser>> {
public:
  using attribute = unused_type;

  explicit not_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute&) const {
    auto i = f; // Do not consume input.
    return !parser_.parse(i, l, unused);
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
