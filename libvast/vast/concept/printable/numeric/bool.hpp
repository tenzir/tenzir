#ifndef VAST_CONCEPT_PRINTABLE_NUMERIC_BOOL_HPP
#define VAST_CONCEPT_PRINTABLE_NUMERIC_BOOL_HPP

#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

// TODO: Customize via policy and merge policies with Parseable concept.
struct bool_printer : printer<bool_printer> {
  using attribute = bool;

  template <typename Iterator>
  bool print(Iterator& out, bool x) const {
    return printers::any.print(out, x ? 'T' : 'F');
  }
};

template <>
struct printer_registry<bool> {
  using type = bool_printer;
};

namespace printers {

auto const tf = bool_printer{};

} // namespace printers
} // namespace vast

#endif
