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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_VALUE_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_VALUE_HPP

#include "vast/value.hpp"
#include "vast/concept/printable/vast/data.hpp"

namespace vast {

struct value_printer : printer<value_printer> {
  using attribute = value;

  template <typename Iterator>
  bool print(Iterator& out, value const& v) const {
    static auto const p = make_printer<data>{};
    return p.print(out, v.data());
  }
};

template <>
struct printer_registry<value> {
  using type = value_printer;
};

} // namespace vast

#endif
