#ifndef VAST_CONCEPT_PARSEABLE_CORE_AS_HPP
#define VAST_CONCEPT_PARSEABLE_CORE_AS_HPP

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

/// Casts a parser's attribute to a specific type.
template <typename Parser, typename Attribute>
class as_parser : public parser<as_parser<Parser, Attribute>> {
public:
  using attribute = Attribute;

  as_parser(Parser p) : parser_{std::move(p)} {
  }

  template <typename Iterator, typename Attr>
  bool parse(Iterator& f, Iterator const& l, Attr& a) const {
    attribute x;
    if (!parser_.parse(f, l, x))
      return false;
    a = std::move(x);
    return true;
  }

private:
  Parser parser_;
};

template <typename Attribute, typename Parser>
auto as(Parser&& p)
-> std::enable_if_t<
     is_parser<std::decay_t<Parser>>::value,
     as_parser<std::decay_t<Parser>, Attribute>
   > {
  return as_parser<std::decay_t<Parser>, Attribute>{std::forward<Parser>(p)};
}

} // namespace vast

#endif

