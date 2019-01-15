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

#include <string>

#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <class Escaper>
struct escape_printer : printer<escape_printer<Escaper>> {
  using attribute = std::string_view;

  explicit escape_printer(Escaper f) : escaper{f} {
    // nop
  }

  template <class Iterator>
  bool print(Iterator& out, std::string_view str) const {
    auto f = str.begin();
    auto l = str.end();
    while (f != l)
      escaper(f, out);
    return true;
  }

  Escaper escaper;
};

namespace printers {

template <class Escaper>
auto escape(Escaper escaper) {
  return escape_printer<Escaper>{escaper};
}

} // namespace printers
} // namespace vast
