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

// -- v0 includes --------------------------------------------------------------

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

#include <cstdint>

namespace vast {

inline namespace v0 {

struct table_slice_column {
  table_slice_column() {
  }

  table_slice_column(table_slice_ptr slice, size_t col)
    : slice{std::move(slice)}, column{col} {
    // nop
  }
  table_slice_ptr slice;
  size_t column;

  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_column& x) {
    return f(x.slice, x.column);
  }
};

} // namespace v0

} // namespace vast
