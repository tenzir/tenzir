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
#include "vast/concept/printable/core/ignore.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

namespace vast {

template <class Bitmap, class Policy = policy::expanded>
struct vector_coder_printer : printer<vector_coder_printer<Bitmap, Policy>> {
  using attribute = vector_coder<Bitmap>;

  template <class Iterator, class Coder>
  bool print(Iterator& out, const Coder& coder) const {
    if (coder.storage().empty())
      return true;
    auto i = size_t{0};
    auto key = printers::integral<size_t> ->* [&] { return i++; };
    auto row = ignore(key) << '\t' << printers::bitmap<Bitmap, Policy>;
    auto bmi = row % '\n';
    return bmi.print(out, coder.storage());
  }
};

template <class Coder>
struct printer_registry<
  Coder,
  std::enable_if_t<
    std::is_base_of<vector_coder<typename Coder::bitmap_type>, Coder>::value
  >
> {
  using type = vector_coder_printer<
    typename Coder::bitmap_type,
    policy::expanded
  >;
};

namespace printers {

template <class Bitmap, class Policy>
auto const vector_coder = vector_coder_printer<Bitmap, Policy>{};

} // namespace printers
} // namespace vast


