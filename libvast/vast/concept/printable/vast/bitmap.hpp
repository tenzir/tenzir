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

#include "vast/bitmap_base.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/bits.hpp"

namespace vast {

template <class Bitmap, class Policy = policy::expanded>
struct bitmap_printer : printer<bitmap_printer<Bitmap, Policy>> {
  using attribute = Bitmap;

  template <class Iterator>
  bool print(Iterator& out, const Bitmap& bm) const {
    auto p = *printers::bits<typename Bitmap::block_type, Policy>;
    return p.print(out, bit_range(bm));
  }
};

template <class Bitmap>
struct printer_registry<
  Bitmap,
  std::enable_if_t<std::is_base_of<bitmap_base<Bitmap>, Bitmap>::value>
> {
  using type = bitmap_printer<Bitmap, policy::expanded>;
};

namespace printers {

template <class Bitmap, class Policy>
auto const bitmap = bitmap_printer<Bitmap, Policy>{};

} // namespace printers
} // namespace vast

