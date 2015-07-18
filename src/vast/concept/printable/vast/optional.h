#ifndef VAST_CONCEPT_PRINTABLE_VAST_OPTIONAL_H
#define VAST_CONCEPT_PRINTABLE_VAST_OPTIONAL_H

#include "vast/error.h"
#include "vast/optional.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/vast/none.h"

namespace vast {

template <typename T>
struct optional_printer : printer<optional_printer<T>>
{
  using attribute = optional<T>;

  template <typename Iterator>
  bool print(Iterator& out, optional<T> const& o) const
  {
    static auto p = make_printer<T>{};
    static auto n = make_printer<none>{};
    return o ? p.print(out, *o) : n.print(out, nil);
  }
};

template <typename T>
struct printer_registry<optional<T>, std::enable_if_t<has_printer<T>{}>>
{
  using type = optional_printer<T>;
};

} // namespace vast

#endif

