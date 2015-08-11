#ifndef VAST_CONCEPT_PRINTABLE_STRING_ANY_H
#define VAST_CONCEPT_PRINTABLE_STRING_ANY_H

#include "vast/concept/printable/core/printer.h"

namespace vast {

struct any_printer : printer<any_printer> {
  using attribute = char;

  template <typename Iterator>
  bool print(Iterator& out, char x) const {
    // TODO: in the future when we have ranges, we should add a mechanism to
    // check whether we exceed the bounds instead of just deref'ing the
    // iterator and pretending it'll work out.
    *out++ = x;
    return true;
  }
};

template <>
struct printer_registry<char> {
  using type = any_printer;
};

namespace printers {

auto const any = any_printer{};

} // namespace printers
} // namespace vast

#endif
