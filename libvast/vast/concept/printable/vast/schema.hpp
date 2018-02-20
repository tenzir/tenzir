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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_SCHEMA_HPP

#include "vast/schema.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/vast/type.hpp"

namespace vast {

struct schema_printer : printer<schema_printer> {
  using attribute = schema;

  template <class Iterator>
  bool print(Iterator& out, const schema& s) const {
    auto p = "type "
          << printers::str
          << " = "
          << printers::type<policy::type_only>
          << '\n';
    for (auto& t : s)
      if (!p(out, t.name(), t))
        return false;
    return true;
  }
};

template <>
struct printer_registry<schema> {
  using type = schema_printer;
};

} // namespace vast

#endif
