#ifndef VAST_CONCEPT_PRINTABLE_STRING_CHAR_HPP
#define VAST_CONCEPT_PRINTABLE_STRING_CHAR_HPP

#include <array>

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <char... Chars>
struct char_printer : printer<char_printer<Chars...>> {
  using attribute = unused_type;

  static constexpr std::array<char, sizeof...(Chars)> chars = {{Chars...}};

  template <typename Iterator>
  bool print(Iterator& out, unused_type) const {
    // TODO: in the future when we have ranges, we should add a mechanism to
    // check whether we exceed the bounds instead of just deref'ing the
    // iterator and pretending it'll work out.
    for (auto c : chars)
      *out++ = c;
    return true;
  }
};

template <char... Chars>
constexpr std::array<char, sizeof...(Chars)> char_printer<Chars...>::chars;

namespace printers {

template <char... Char>
auto chr = char_printer<Char...>{};

} // namespace printers
} // namespace vast

#endif
