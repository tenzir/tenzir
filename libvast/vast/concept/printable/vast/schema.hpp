#ifndef VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_HPP

#include "vast/schema.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/vast/type.hpp"

namespace vast {

struct schema_printer : printer<schema_printer> {
  using attribute = schema;

  template <typename Iterator>
  bool print(Iterator& out, schema const& s) const {
    using namespace printers;
    for (auto& t : s)
      if (!t.name().empty())
        if (!(str.print(out, "type ") && str.print(out, t.name())
              && str.print(out, " = ")
              && printers::type<policy::type_only>.print(out, t)
              && any.print(out, '\n')))
          return false;
    return true;
  }
};

template <>
struct printer_registry<schema> {
  using type = schema_printer;
};

} // namespace vast

#endif
