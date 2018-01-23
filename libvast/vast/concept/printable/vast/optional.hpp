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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_OPTIONAL_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_OPTIONAL_HPP

#include "vast/error.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/vast/none.hpp"

namespace vast {

template <typename T>
struct optional_printer : printer<optional_printer<T>> {
  using attribute = optional<T>;

  template <typename Iterator>
  bool print(Iterator& out, const optional<T>& o) const {
    static auto p = make_printer<T>{};
    static auto n = make_printer<none>{};
    return o ? p.print(out, *o) : n.print(out, nil);
  }
};

template <typename T>
struct printer_registry<optional<T>, std::enable_if_t<has_printer<T>{}>> {
  using type = optional_printer<T>;
};

} // namespace vast

#endif
