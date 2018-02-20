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

#ifndef VAST_CONCEPT_PRINTABLE_NUMERIC_BOOL_HPP
#define VAST_CONCEPT_PRINTABLE_NUMERIC_BOOL_HPP

#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

// TODO: Customize via policy and merge policies with Parseable concept.
struct bool_printer : printer<bool_printer> {
  using attribute = bool;

  template <class Iterator>
  bool print(Iterator& out, bool x) const {
    return printers::any.print(out, x ? 'T' : 'F');
  }
};

template <>
struct printer_registry<bool> {
  using type = bool_printer;
};

namespace printers {

auto const tf = bool_printer{};

} // namespace printers
} // namespace vast

#endif
