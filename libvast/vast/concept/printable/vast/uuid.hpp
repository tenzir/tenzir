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

#ifndef VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_UUID_HPP

#include "vast/access.hpp"
#include "vast/uuid.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/detail/coding.hpp"

namespace vast {

template <>
struct access::printer<uuid> : vast::printer<access::printer<uuid>> {
  using attribute = uuid;

  template <typename Iterator>
  bool print(Iterator& out, const uuid& u) const {
    static auto byte = printers::any << printers::any;
    for (size_t i = 0; i < 16; ++i) {
      auto hi = detail::byte_to_char((u.id_[i] >> 4) & 0x0f);
      auto lo = detail::byte_to_char(u.id_[i] & 0x0f);
      if (!byte(out, hi, lo))
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
  using type = access::printer<uuid>;
};

} // namespace vast

#endif
