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

#include "vast/die.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fwd.hpp"

#include <functional>
#include <type_traits>

namespace vast {

/// Visits a FlatBuffers table slice to dispatch to its specific encoding.
/// @param visitor A callable object to dispatch to.
/// @param x The FlatBuffers root type for table slices.
/// @note The handler for invalid table slices takes no arguments. If none is
/// specified, visig aborts when the table slice encoding is invalid.
template <class Visitor>
auto visit(Visitor&& visitor, const fbs::TableSlice* x) noexcept(
  std::conjunction_v<
    // Check whether the handler for invalid encodings is noexcept-specified,
    // if and only if it exists.
    std::disjunction<std::negation<std::is_invocable<Visitor>>,
                     std::is_nothrow_invocable<Visitor>>,
    // Check whether the handlers for all other table slice encodings are
    // noexcept-specified. When adding a new encoding, add it here as well.
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::legacy::v0*>>) {
  if (!x) {
    if constexpr (std::is_invocable_v<Visitor>)
      return std::invoke(std::forward<Visitor>(visitor));
    else
      die("visitor cannot handle invalid table slices");
  }
  switch (x->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE:
      if constexpr (std::is_invocable_v<Visitor>)
        return std::invoke(std::forward<Visitor>(visitor));
      else
        die("visitor cannot handle table slices with an invalid encoding");
    case fbs::table_slice::TableSlice::legacy_v0:
      return std::invoke(std::forward<Visitor>(visitor),
                         x->table_slice_as_legacy_v0());
  }
  // GCC-8 fails to recognize that this can never be reached, so we just call a
  // [[noreturn]] function.
  die("unhandled table slice encoding");
}

} // namespace vast
