#ifndef VAST_CONCEPT_PRINTABLE_VAST_EVENT_H
#define VAST_CONCEPT_PRINTABLE_VAST_EVENT_H

#include "vast/event.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/numeric/integral.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/string/any.h"
#include "vast/concept/printable/vast/value.h"
#include "vast/concept/printable/vast/time.h"

namespace vast {

struct event_printer : printer<event_printer>
{
  using attribute = event;

  template <typename Iterator>
  bool print(Iterator& out, event const& e) const
  {
    using namespace printers;
    if (e.type().name().empty() && ! str.print(out, "<anonymous>"))
      return false;
    return str.print(out, e.type().name())
        && str.print(out, " [")
        && u64.print(out, e.id())
        && any.print(out, '|')
        && make_printer<time::point>{}.print(out, e.timestamp())
        && str.print(out, "] ")
        && make_printer<value>{}.print(out, e);
  }
};

template <>
struct printer_registry<event>
{
  using type = event_printer;
};

} // namespace vast

#endif
