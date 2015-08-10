#ifndef VAST_CONCEPT_PRINTABLE_VAST_VALUE_H
#define VAST_CONCEPT_PRINTABLE_VAST_VALUE_H

#include "vast/value.h"
#include "vast/concept/printable/vast/data.h"

namespace vast {

struct value_printer : printer<value_printer> {
  using attribute = value;

  template <typename Iterator>
  bool print(Iterator& out, value const& v) const {
    static auto const p = make_printer<data>{};
    return p.print(out, v.data());
  }
};

template <>
struct printer_registry<value> {
  using type = value_printer;
};

} // namespace vast

#endif
