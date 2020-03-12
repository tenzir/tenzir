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
#include "vast/error.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/version.hpp"
#include "vast/fwd.hpp"
#include "vast/span.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstdint>

#include <flatbuffers/flatbuffers.h>

namespace vast::fbs {

// -- general helpers --------------------------------------------------------

/// Releases the buffer of finished builder in the form of a chunk.
/// @param builder The finished builder.
/// @returns The buffer of *builder*.
chunk_ptr release(flatbuffers::FlatBufferBuilder& builder);

/// Creates a verifier for a chunk.
/// @chk The chk to initialize the verifier with.
/// @param A verifier that is ready to use.
flatbuffers::Verifier make_verifier(chunk_ptr chk);

/// Performs a check whether two given versions are equal and returns an error
/// if not.
/// @param given The provided version to check.
/// @param expected The version that *given* should be.
/// @returns An error iff *given* does not match *expected*.
caf::error check_version(Version given, Version expected);

/// Converts a flatbuffer vector of [u]int8_t into a byte span.
/// @param xs The flatbuffer to convert.
/// @returns A byte span of *xs*.
template <size_t Extent = dynamic_extent, class T>
span<const byte, Extent> as_bytes(const flatbuffers::Vector<T>& xs) {
  static_assert(sizeof(T) == 1, "only byte vectors supported");
  VAST_ASSERT(xs.size() <= Extent);
  auto data = reinterpret_cast<const byte*>(xs.data());
  return span<const byte, Extent>(data, Extent);
}

// -- builder utilities ------------------------------------------------------

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

caf::expected<caf::atom_value> unpack(Encoding x);

caf::expected<table_slice_ptr> unpack(const TableSlice& x);

} // namespace vast::fbs
