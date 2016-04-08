#ifndef VAST_CONCEPT_PARSEABLE_CORE_AND_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_AND_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

// The AND parser does not consume its input and serves as basic look-ahead.
template <typename Parser>
class and_parser : public parser<and_parser<Parser>> {
public:
  using attribute = unused_type;

  explicit and_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute&) const {
    auto i = f; // Do not consume input.
    return parser_.parse(i, l, unused);
  }

private:
  Parser parser_;
};

} // namespace vast

#endif
