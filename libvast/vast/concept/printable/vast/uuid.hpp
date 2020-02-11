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
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/detail/coding.hpp"
#include "vast/uuid.hpp"

namespace vast {

struct uuid_printer : vast::printer<uuid_printer> {
  using attribute = uuid;

  static constexpr auto hexbyte = printers::any << printers::any;

  template <class Iterator>
  bool print(Iterator& out, const uuid& x) const {
    for (size_t i = 0; i < 16; ++i) {
      auto hi = detail::byte_to_char((x[i] >> 4) & byte{0x0f});
      auto lo = detail::byte_to_char(x[i] & byte{0x0f});
      if (!hexbyte(out, hi, lo))
        return false;
      if (i == 3 || i == 5 || i == 7 || i == 9)
        if (!printers::chr<'-'>(out))
          return false;
    }
    return true;
  }
};

template <>
struct printer_registry<uuid> {
  using type = uuid_printer;
};

} // namespace vast

