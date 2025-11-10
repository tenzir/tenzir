//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/chunk.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace tenzir::fbs {

// -- general helpers --------------------------------------------------------

/// Releases the buffer of finished builder in the form of a chunk.
/// @param builder The finished builder.
/// @returns The buffer of *builder*.
chunk_ptr release(flatbuffers::FlatBufferBuilder& builder);

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

/// Adds a byte vector to the builder for a type that can be serialized to a
/// byte sequence using the `caf::binary_serializer`.
template <class T, class Byte = uint8_t>
caf::expected<flatbuffers::Offset<flatbuffers::Vector<Byte>>>
serialize_bytes(flatbuffers::FlatBufferBuilder& builder, const T& x) {
  static_assert(detail::is_any_v<Byte, int8_t, uint8_t>);
  caf::byte_buffer buf;
  caf::binary_serializer source(buf);
  if (not source.apply(x)) {
    return caf::make_error(ec::serialization_error, "failed to serialize "
                                                    "bytes");
  }
  return builder.CreateVector(reinterpret_cast<const Byte*>(buf.data()),
                              buf.size());
}

/// Deserializes an object of type `T` from a flatbuffer byte vector, using
/// the `detail::legacy_deserializer`.
template <class T, class Byte = uint8_t>
caf::error deserialize_bytes(const flatbuffers::Vector<Byte>* v, T& x) {
  static_assert(detail::is_any_v<Byte, int8_t, uint8_t>);
  if (not v) {
    return caf::make_error(ec::format_error, "no input");
  }
  detail::legacy_deserializer sink(as_bytes(*v));
  if (not sink.apply(x)) {
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to deserialize {}",
                                       detail::pretty_type_name(x)));
  }
  return caf::none;
}

/// Generic unpacking utility. The structural integrity of the flatbuffer is
/// verified (i.e., no out-of-bounds offsets), but no type checking is done
/// at all.
/// @tparam Flatbuffer The flatbuffer type to unpack.
/// @param xs The buffer to unpack a flatbuffer from.
/// @returns A pointer to the unpacked flatbuffer of type `Flatbuffer` or
///          `nullptr` if verification failed.
template <class Flatbuffer, size_t Extent = dynamic_extent>
const Flatbuffer* as_flatbuffer(std::span<const std::byte, Extent> xs) {
  // Verify the buffer.
  auto data = reinterpret_cast<const uint8_t*>(xs.data());
  auto size = xs.size();
  flatbuffers::Verifier verifier{data, size};
  if (not verifier.template VerifyBuffer<Flatbuffer>()) {
    return nullptr;
  }
  return flatbuffers::GetRoot<Flatbuffer>(data);
}

/// Wraps an object into a flatbuffer. This function requires existence of an
/// overload `pack(flatbuffers::FlatBufferBuilder&, const T&)` that can be
/// found via ADL. While *packing* incrementally adds objects to a builder,
/// whereas *wrapping* produces the final buffer for use in subsequent
/// operations.
/// @param x The object to wrap.
/// @param file_identifier The identifier of the generated flatbuffer.
///        This is necessary because it's impossible to figure out via type
///        introspection whether a flatbuffer root type has a file_identifier.
///        See the documentation for `tenzir::fbs::file_identifier` for details.
/// @returns The buffer containing the flatbuffer of *x*.
/// @relates file_identifier
template <class T>
caf::expected<chunk_ptr>
wrap(T const& x, const char* file_identifier = nullptr) {
  flatbuffers::FlatBufferBuilder builder;
  auto root = pack(builder, x);
  if (not root) {
    return root.error();
  }
  builder.Finish(*root, file_identifier);
  return release(builder);
}

/// Unwraps a flatbuffer into an object. This function requires existence of an
/// overload `unpack(const T&, const U&` where `T` is a generated flatbuffer
/// type and `U` the Tenzir type.
/// @param xs The byte buffer that contains the flatbuffer.
/// @param x The object to unpack *xs* into.
/// @returns An error iff the operation failed.
template <class Flatbuffer, size_t Extent = dynamic_extent, class T>
[[nodiscard]] caf::error unwrap(std::span<const std::byte, Extent> xs, T& x) {
  if (auto flatbuf = as_flatbuffer<Flatbuffer>(xs)) {
    return unpack(*flatbuf, x);
  }
  return caf::make_error(ec::unspecified, "flatbuffer verification failed");
}

/// Unwraps a flatbuffer and returns a new object. This function works as the
/// corresponding two-argument overload, but returns the unwrapped object
/// instead of taking it as argument.
template <class Flatbuffer, size_t Extent = dynamic_extent, class T>
caf::expected<T> unwrap(std::span<const std::byte, Extent> xs) {
  T result;
  if (auto err = unwrap(xs, result)) {
    return err;
  }
  return result;
}

} // namespace tenzir::fbs
