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

#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/format/printer_writer.hpp"
#include "vast/json.hpp"

namespace vast::format::json {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    vast::json j;
    if (!convert(e.data(), j, e.type()))
      return false;
    return printers::json<policy::oneline>.print(out, j);
  }
};

class writer : public printer_writer<event_printer>{
public:
  using printer_writer<event_printer>::printer_writer;

  const char* name() const {
    return "json-writer";
  }
};

} // namespace vast::format::json
