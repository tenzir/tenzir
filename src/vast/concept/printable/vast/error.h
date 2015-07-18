#ifndef VAST_CONCEPT_PRINTABLE_VAST_ERROR_H
#define VAST_CONCEPT_PRINTABLE_VAST_ERROR_H

#include "vast/error.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/string.h"

namespace vast {

struct error_printer : printer<error_printer>
{
  using attribute = error;

  template <typename Iterator>
  bool print(Iterator& out, error const& e) const
  {
    return printers::str.print(out, e.msg());
  }
};

template <>
struct printer_registry<error>
{
  using type = error_printer;
};

} // namespace vast

#endif
