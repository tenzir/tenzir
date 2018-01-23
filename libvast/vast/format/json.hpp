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

#ifndef VAST_FORMAT_JSON_HPP
#define VAST_FORMAT_JSON_HPP

#include "vast/json.hpp"
#include "vast/concept/printable/vast/json.hpp"

#include "vast/format/writer.hpp"

namespace vast::format::json {

struct event_printer : printer<event_printer> {
  using attribute = event;

  template <class Iterator>
  bool print(Iterator& out, const event& e) const {
    vast::json j;
    return convert(e, j) && printers::json<policy::oneline>.print(out, j);
  }
};

class writer : public format::writer<event_printer>{
public:
  using format::writer<event_printer>::writer;

  char const* name() const {
    return "json-writer";
  }
};

} // namespace vast::format::json

#endif


