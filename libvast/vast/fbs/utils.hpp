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

#include "vast/byte.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include <caf/expected.hpp>

#include <cstdint>

#include <flatbuffers/flatbuffers.h>

namespace vast::fbs {

// The utility functions in this header have the following naming convention:
// - `pack*` are functions to convert from VAST to flatbuffers
// - `unpack*` are functions to convert from flatbuffers to VAST

template <class T, class Byte = uint8_t>
flatbuffers::Offset<flatbuffers::Vector<Byte>>
pack_bytes(flatbuffers::FlatBufferBuilder& builder, const T& x) {
  static_assert(detail::is_any_v<Byte, int8_t, uint8_t>);
  auto bytes = as_bytes(x);
  auto data = reinterpret_cast<const Byte*>(bytes.data());
  return builder.CreateVector(data, bytes.size());
}

caf::expected<flatbuffers::Offset<TableSliceBuffer>>
pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x);

caf::atom_value unpack(Encoding x);

table_slice_ptr unpack(const TableSlice& x);

} // namespace vast::fbs
