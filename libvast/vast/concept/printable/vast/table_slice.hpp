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
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/string/char.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// Prints a table slice as ID interval.
struct table_slice_printer : printer<table_slice_printer> {
  using attribute = table_slice;

  template <class Iterator>
  bool print(Iterator& out, const table_slice& x) const {
    using namespace printers;
    auto p = '[' << u64 << ',' << u64 << ')';
    return p(out, x.offset(), x.offset() + x.rows());
  }
};

template <>
struct printer_registry<table_slice> {
  using type = table_slice_printer;
};

} // namespace vast
