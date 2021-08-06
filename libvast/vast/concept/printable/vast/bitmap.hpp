//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitmap_base.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/vast/bits.hpp"

namespace vast {

template <class Bitmap, class Policy = policy::expanded>
struct bitmap_printer : printer_base<bitmap_printer<Bitmap, Policy>> {
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
  std::enable_if_t<std::is_base_of_v<bitmap_base<Bitmap>, Bitmap>>
> {
  using type = bitmap_printer<Bitmap, policy::expanded>;
};

namespace printers {

template <class Bitmap, class Policy>
auto const bitmap = bitmap_printer<Bitmap, Policy>{};

} // namespace printers
} // namespace vast

