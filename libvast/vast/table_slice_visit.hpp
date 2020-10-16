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

#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_encoding.hpp"

namespace vast {

namespace v1 {

/// Visit a table slice for to access its underlying versioned and
/// encoding-specific FlatBuffers table.
/// @param visitor An invocable object that is dispatched to.
/// @param slice The table slice to visit.
/// @pre `slice.chunk()`
/// @relates table_slice
template <class Visitor>
auto visit(Visitor&& visitor, const table_slice& slice) noexcept(
  std::conjunction_v<
    std::is_nothrow_invocable<Visitor>,
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::msgpack::v0&>>) {
  VAST_ASSERT(slice.chunk());
  // The actual dispatching function that ends up calling the vsitor.
  auto dispatch = [&](auto&&... args) {
    // Note for maintainers: We use an IIFE here such that we do not need to
    // pass the visitor to dispatch by hand below. We need it as an argument in
    // the actual dispatching logic, a lambda capture fails because the
    // `detail::always_false_v<decltype(visitor)>` gets instantiated too early,
    // causing the static assertion to trigger when it shouldn't.
    return [](auto&& visitor, auto&&... args) {
      if constexpr (std::is_invocable_v<decltype(visitor), decltype(args)...>)
        return std::invoke(std::forward<decltype(visitor)>(visitor),
                           std::forward<decltype(args)>(args)...);
      else
        static_assert(detail::always_false_v<decltype(visitor)>,
                      "Visitor must accept either no arguments or a versioned "
                      "and encoding-specific FlatBuffers table");
    }(std::forward<Visitor>(visitor), std::forward<decltype(args)>(args)...);
  };
  auto fbs_slice = fbs::as_flatbuffer<fbs::TableSlice>(as_bytes(slice.chunk()));
  if (!fbs_slice)
    return dispatch();
  // Note for maintainers: When adding a new table slice version/encoding pair
  // here, make sure to also add it to the noexcept condition above.
  switch (fbs_slice->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE: {
      return dispatch();
    }
    case fbs::table_slice::TableSlice::generic_v0: {
      die("unable to dispatch vast.fbs.table_slice.generic.v0");
    }
    case fbs::table_slice::TableSlice::msgpack_v0: {
      return dispatch(*fbs_slice->table_slice_as_msgpack_v0());
    }
  }
}

} // namespace v1

} // namespace vast
