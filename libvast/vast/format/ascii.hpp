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

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/format/printer_writer.hpp"

namespace vast::format::ascii {

struct ascii_printer : printer<ascii_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator&& out, const event& e) const {
    return event_printer{}.print(out, e);
  }
};

class writer : public printer_writer<ascii_printer>{
public:
  using printer_writer<ascii_printer>::printer_writer;

  const char* name() const override {
    return "ascii-writer";
  }
};

} // namespace vast::format::ascii
