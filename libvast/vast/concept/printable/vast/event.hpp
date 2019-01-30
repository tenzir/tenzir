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

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/event.hpp"

namespace vast {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    auto p = '<' << (printers::data % ", ") << '>';
    auto& xs = caf::get<vector>(e.data());
    return p(out, xs);
  }
};

template <>
struct printer_registry<event> {
  using type = event_printer;
};

} // namespace vast
