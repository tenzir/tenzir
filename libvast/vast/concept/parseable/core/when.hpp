#ifndef VAST_CONCEPT_PARSEABLE_CORE_WHEN_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_WHEN_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <class Parser, class Condition>
class when_parser : public parser<when_parser<Parser, Condition>> {
public:
  using attribute = typename Parser::attribute;

  when_parser(Parser p, Condition fun) : parser_{std::move(p)}, condition_(fun) {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return condition_() && parser_(f, l, x);
  }

private:
  Parser parser_;
  Condition condition_;
};

} // namespace vast

#endif

