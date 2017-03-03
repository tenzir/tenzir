#ifndef VAST_CONCEPT_PRINTABLE_VAST_EVENT_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_EVENT_HPP

#include "vast/event.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/vast/value.hpp"

namespace vast {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <typename Iterator>
  bool print(Iterator& out, event const& e) const {
    using namespace printers;
    static auto p = str << str << u64 << chr<'|'>
                    << make_printer<timestamp>{} << str
                    << make_printer<value>{};
    if (e.type().name().empty() && !str(out, "<anonymous>"))
      return false;
    return p(out, e.type().name(), " [", e.id(), e.timestamp(), "] ", e);
  }
};

template <>
struct printer_registry<event> {
  using type = event_printer;
};

} // namespace vast

#endif
