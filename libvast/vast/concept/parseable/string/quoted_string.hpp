#ifndef VAST_CONCEPT_PARSEABLE_STRING_QUOTED_STRING_HPP
#define VAST_CONCEPT_PARSEABLE_STRING_QUOTED_STRING_HPP

#include <cassert>
#include <string>

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

template <char Quote, char Esc = '\\'>
class quoted_string_parser : public parser<quoted_string_parser<Quote, Esc>> {
public:
  using attribute = std::string;

  quoted_string_parser() = default;

  static auto make() {
    auto escaped_quote = Esc >> char_parser{Quote};
    return Quote >> +(escaped_quote | (parsers::print - Quote)) >> Quote;
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    static auto p = make();
    return p.parse(f, l, a);
  }
};

namespace parsers {

auto const q_str = quoted_string_parser<'\'', '\\'>{};
auto const qq_str = quoted_string_parser<'"', '\\'>{};

} // namespace parsers

template <>
struct parser_registry<std::string> {
  using type = quoted_string_parser<'"', '\\'>;
};

} // namespace vast

#endif
