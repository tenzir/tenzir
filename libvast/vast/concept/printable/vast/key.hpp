#ifndef VAST_CONCEPT_PRINTABLE_VAST_KEY_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_KEY_HPP

#include "vast/key.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"

namespace vast {

struct key_printer : printer<key_printer> {
  using attribute = key;

  template <typename Iterator>
  bool print(Iterator& out, key const& k) const {
    return detail::print_delimited(k.begin(), k.end(), out, key::delimiter);
  }
};

template <>
struct printer_registry<key> {
  using type = key_printer;
};

namespace printers {
  auto const key = key_printer{};
} // namespace printers

} // namespace vast

#endif
