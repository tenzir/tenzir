#ifndef VAST_CONCEPT_PRINTABLE_VAST_ERROR_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_ERROR_HPP

#include "vast/error.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

struct error_printer : printer<error_printer> {
  using attribute = error;

  template <typename Iterator>
  bool print(Iterator& out, error const& e) const {
    auto msg = to_string(e);
    return printers::str.print(out, msg);
  }
};

template <>
struct printer_registry<error> {
  using type = error_printer;
};

} // namespace vast

#endif
