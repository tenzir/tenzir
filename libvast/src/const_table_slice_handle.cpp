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

#include "vast/const_table_slice_handle.hpp"

#include <caf/detail/scope_guard.hpp>
#include <caf/error.hpp>

#include "vast/table_slice.hpp"
#include "vast/table_slice_handle.hpp"

namespace vast {

const_table_slice_handle::const_table_slice_handle(
  const table_slice_handle& other)
  : super(other.ptr()) {
  // nop
}

const_table_slice_handle::~const_table_slice_handle() {
  // nop
}

caf::error inspect(caf::serializer& sink, const_table_slice_handle& hdl) {
  return table_slice::serialize_ptr(sink, hdl.get());
}

caf::error inspect(caf::deserializer& source, const_table_slice_handle& hdl) {
  table_slice_ptr ptr;
  auto guard = caf::detail::make_scope_guard([&] {
    hdl = const_table_slice_handle{std::move(ptr)};
  });
  return table_slice::deserialize_ptr(source, ptr);
}

} // namespace vast
