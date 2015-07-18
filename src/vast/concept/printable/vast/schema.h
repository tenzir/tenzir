#ifndef VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_H
#define VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_H

#include "vast/schema.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/char.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/vast/type.h"

namespace vast {

struct schema_printer : printer<schema_printer>
{
  using attribute = schema;

  template <typename Iterator>
  bool print(Iterator& out, schema const& s) const
  {
    using namespace printers;
    for (auto& t : s)
      if (! t.name().empty())
        if (! (str.print(out, "type ")
               && str.print(out, t.name())
               && str.print(out, " = ")
               && printers::type<policy::type_only>.print(out, t)
               && any.print(out, '\n')))
          return false;
    return true;
  }
};

template <>
struct printer_registry<schema>
{
  using type = schema_printer;
};

} // namespace vast

#endif
