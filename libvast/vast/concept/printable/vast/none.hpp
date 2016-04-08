#ifndef VAST_CONCEPT_PRINTABLE_VAST_NONE_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_NONE_HPP

#include "vast/none.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

struct none_printer : printer<none_printer> {
  using attribute = none;

  template <typename Iterator>
  bool print(Iterator& out, none) const {
    return printers::str.print(out, "nil");
  }
};

template <>
struct printer_registry<none> {
  using type = none_printer;
};

} // namespace vast

#endif
