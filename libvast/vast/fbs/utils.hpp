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
#include "vast/fbs/meta_index.hpp"
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

/// Creates a verifier for a byte buffer.
/// @chk The chk to initialize the verifier with.
/// @param A verifier that is ready to use.
[[deprecated("use make_verifier(as_bytes(xs)) instead")]] flatbuffers::Verifier
make_verifier(chunk_ptr chk);

/// Creates a verifier for a byte buffer.
/// @xs The buffer to create a verifier for.
/// @param A verifier that is ready to use.
flatbuffers::Verifier make_verifier(span<const byte> xs);

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
  auto data = reinterpret_cast<const byte*>(xs.data());
  return span<const byte, Extent>(data, Extent);
}

/// Retrieves the internal buffer of a builder.
/// @param builder The builder to access.
/// @returns A span of bytes of the buffer of *builder*.
span<const byte> as_bytes(const flatbuffers::FlatBufferBuilder& builder);

// -- VAST-specific (un)packing ----------------------------------------------

caf::expected<flatbuffers::Offset<TableSliceBuffer>>
pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x);

caf::expected<caf::atom_value> unpack(Encoding x);

caf::expected<table_slice_ptr> unpack(const TableSlice& x);

caf::expected<flatbuffers::Offset<MetaIndex>>
pack(flatbuffers::FlatBufferBuilder& builder, const meta_index& x);

caf::expected<meta_index> unpack(const MetaIndex& x);

// -- generic (un)packing ----------------------------------------------------

/// Adds a byte vector to builder for a type that is convertible to a byte
/// sequence via `as_bytes`.
/// @param builder The builder to append *x* to.
/// @param x The instance to append to *builder* in the form of `as_bytes(x)`.
/// @returns The flatbuffer offset for *x*.
template <class T, class Byte = uint8_t>
flatbuffers::Offset<flatbuffers::Vector<Byte>>
pack_bytes(flatbuffers::FlatBufferBuilder& builder, const T& x) {
  static_assert(detail::is_any_v<Byte, int8_t, uint8_t>);
  auto bytes = as_bytes(x);
  auto data = reinterpret_cast<const Byte*>(bytes.data());
  return builder.CreateVector(data, bytes.size());
}

/// Generic unpacking utility.
/// @tparam Flatbuffer The flatbuffer type to unpack.
/// @param xs The buffer to unpack a flatbuffer from.
/// @returns A pointer to the unpacked flatbuffer of type `Flatbuffer` or
///          `nullptr` if verification failed.
/// @relates unverified_unpack
template <class Flatbuffer, size_t Extent = dynamic_extent>
const Flatbuffer* as_flatbuffer(span<const byte, Extent> xs) {
  /// The flatbuffer file_identifier. Defining this identifier here is
  /// workaround for the lack of traits for generated flatbuffer classes. The
  /// only way to get a flatbuffer identifier is by calling MyTypeIdentifier().
  /// Because this prevents use of generic programming, we only use one global
  /// file identifier for VAST flatbuffers.
  constexpr const char* file_identifier = "VAST";
  // Verify the buffer.
  auto data = reinterpret_cast<const uint8_t*>(xs.data());
  auto size = xs.size();
  char const* identifier = nullptr;
  if (flatbuffers::BufferHasIdentifier(data, file_identifier))
    identifier = file_identifier;
  flatbuffers::Verifier verifier{data, size};
  if (!verifier.template VerifyBuffer<Flatbuffer>(identifier))
    return nullptr;
  return flatbuffers::GetRoot<Flatbuffer>(data);
}

/// Wraps an object into a flatbuffer.
template <class T>
caf::expected<chunk_ptr>
wrap(T const& x, const char* file_identifier = nullptr) {
  using fbs::pack;
  flatbuffers::FlatBufferBuilder builder;
  auto root = pack(builder, x);
  if (!root)
    return root.error();
  builder.Finish(*root, file_identifier);
  return release(builder);
}

/// Unwraps an objects from a flatbuffer.
template <class Flatbuffer, size_t Extent = dynamic_extent>
decltype(unpack(std::declval<const Flatbuffer&>()))
unwrap(span<const byte, Extent> xs) {
  using fbs::unpack;
  if (auto flatbuf = as_flatbuffer<Flatbuffer>(xs))
    return unpack(*flatbuf);
  return make_error(ec::unspecified, "flatbuffer verification failed");
}

} // namespace vast::fbs
