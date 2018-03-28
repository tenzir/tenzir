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

#include "vast/offset.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/concept/printable/numeric/integral.hpp"

namespace vast {

struct offset_printer : printer<offset_printer> {
  using attribute = offset;

  template <class Iterator>
  bool print(Iterator& out, const offset& o) const {
    using delim = char_printer<','>;
    return detail::print_delimited<size_t, delim>(o.begin(), o.end(), out);
  }
};

template <>
struct printer_registry<offset> {
  using type = offset_printer;
};

namespace printers {
  auto const offset = offset_printer{};
} // namespace printers

} // namespace vast

