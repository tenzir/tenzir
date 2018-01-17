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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_KEY_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_KEY_HPP

#include "vast/key.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"

namespace vast {

struct key_printer : printer<key_printer> {
  using attribute = key;

  template <typename Iterator>
  bool print(Iterator& out, key const& k) const {
    return detail::print_delimited(k.begin(), k.end(), out, key::delimiter);
  }
};

template <>
struct printer_registry<key> {
  using type = key_printer;
};

namespace printers {
  auto const key = key_printer{};
} // namespace printers

} // namespace vast

#endif
