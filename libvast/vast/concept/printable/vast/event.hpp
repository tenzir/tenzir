/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
