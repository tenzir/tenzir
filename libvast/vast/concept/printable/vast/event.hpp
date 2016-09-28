#ifndef VAST_CONCEPT_PRINTABLE_VAST_EVENT_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_EVENT_HPP

#include "vast/event.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/vast/value.hpp"

namespace vast {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <typename Iterator>
  bool print(Iterator& out, event const& e) const {
    using namespace printers;
    if (e.type().name().empty() && !str.print(out, "<anonymous>"))
      return false;
    return str.print(out, e.type().name()) && str.print(out, " [")
           && u64.print(out, e.id()) && any.print(out, '|')
           && make_printer<timestamp>{}.print(out, e.timestamp())
           && str.print(out, "] ") && make_printer<value>{}.print(out, e);
  }
};

template <>
struct printer_registry<event> {
  using type = event_printer;
};

} // namespace vast

#endif
