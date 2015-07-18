#ifndef VAST_CONCEPT_PRINTABLE_VAST_NONE_H
#define VAST_CONCEPT_PRINTABLE_VAST_NONE_H

#include "vast/none.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/string.h"

namespace vast {

struct none_printer : printer<none_printer>
{
  using attribute = none;

  template <typename Iterator>
  bool print(Iterator& out, none) const
  {
    return printers::str.print(out, "nil");
  }
};

template <>
struct printer_registry<none>
{
  using type = none_printer;
};

} // namespace vast

#endif

