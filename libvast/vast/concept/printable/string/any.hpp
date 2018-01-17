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

#ifndef VAST_CONCEPT_PRINTABLE_STRING_ANY_HPP
#define VAST_CONCEPT_PRINTABLE_STRING_ANY_HPP

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

struct any_printer : printer<any_printer> {
  using attribute = char;

  template <typename Iterator>
  bool print(Iterator& out, char x) const {
    // TODO: in the future when we have ranges, we should add a mechanism to
    // check whether we exceed the bounds instead of just deref'ing the
    // iterator and pretending it'll work out.
    *out++ = x;
    return true;
  }
};

template <>
struct printer_registry<char> {
  using type = any_printer;
};

namespace printers {

auto const any = any_printer{};

} // namespace printers
} // namespace vast

#endif
