//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/generator.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace tenzir::nova::bitz {

/// Resource limits for decoding one Bitz v2 payload.
///
/// Limits apply cumulatively to the complete recursive payload where noted.
/// Callers may raise them for trusted inputs, but should not disable them for
/// data received from external sources.
struct DecodeLimits {
  std::uint64_t max_frame_bytes = std::uint64_t{64} << 20;
  std::uint32_t max_rows = std::uint32_t{1} << 20;
  std::uint32_t max_array_length = std::uint32_t{16} << 20;
  std::uint32_t max_nesting = 64;
  std::uint32_t max_array_nodes = std::uint32_t{64} << 10;
  std::uint32_t max_fields_per_record = std::uint32_t{16} << 10;
  std::uint32_t max_shapes_per_record = std::uint32_t{64} << 10;
  std::uint64_t max_shape_entries = std::uint64_t{1} << 20;
  std::uint32_t max_field_name_bytes = std::uint32_t{64} << 10;
  std::uint64_t max_total_name_bytes = std::uint64_t{16} << 20;
  std::uint64_t max_logical_slots = std::uint64_t{32} << 20;
  std::uint64_t max_decoded_bytes = std::uint64_t{256} << 20;
};

inline constexpr auto default_decode_limits = DecodeLimits{};

/// The byte order for fixed-width scalar array bodies.
///
/// Structural values remain little-endian regardless of this setting.
enum class ScalarByteOrder : std::uint8_t {
  little = 0,
  big = 1,
};

inline constexpr auto native_scalar_byte_order = [] {
  static_assert(std::endian::native == std::endian::little
                or std::endian::native == std::endian::big);
  if constexpr (std::endian::native == std::endian::little) {
    return ScalarByteOrder::little;
  } else {
    return ScalarByteOrder::big;
  }
}();

struct EncodeOptions {
  ScalarByteOrder scalar_byte_order = native_scalar_byte_order;
  std::uint32_t max_rows = default_decode_limits.max_rows;
  std::uint32_t max_array_length = default_decode_limits.max_array_length;
  std::uint64_t max_frame_bytes = default_decode_limits.max_frame_bytes;
  std::uint64_t max_logical_slots = default_decode_limits.max_logical_slots;
  std::uint64_t max_decoded_bytes = default_decode_limits.max_decoded_bytes;
};

/// The stable logical type identifiers used by the Bitz v2 payload codec.
///
/// These values are part of the wire format and must never be renumbered.
enum class TypeId : std::uint8_t {
  null = 0,
  boolean = 1,
  integer = 2,
  unsigned_integer = 3,
  floating_point = 4,
  string = 5,
  blob = 6,
  ip = 7,
  subnet = 8,
  time = 9,
  duration = 10,
  list = 11,
  record = 12,
  union_ = 13,
};

/// A generalized batch of events.
struct Batch {
  Array<Data> data;
  storage::BitMap mask;
  Events::Meta meta;

  auto length() const noexcept -> storage::Index {
    return data.length();
  }
};

/// Encodes one unframed, uncompressed Bitz v2 payload.
///
/// The encoding is recursively columnar. Structural values are little-endian.
/// Fixed-width scalar array bodies use the configured byte order, which
/// defaults to the host's native order. Each type selects a physical encoding
/// without requiring the decoder to retain it. Visibility is propagated
/// through nested arrays, and inaccessible positions receive placeholders
/// without changing physical row or child-array positions. The encoder writes
/// into a buffer that grows geometrically from an estimate of the batch's
/// in-memory size, enforcing the default decoder's resource limits as it goes:
/// cumulative budgets and visibility validation memory are charged during the
/// single pass, and a payload that would exceed the frame limit stops being
/// written at the limit rather than being allocated and then rejected.
/// Trusted callers can explicitly raise size limits in `options`, but must use
/// matching decoder limits.
[[nodiscard]] auto encode(Batch const& batch, EncodeOptions const& options = {})
  -> Result<std::vector<std::byte>, std::string>;

/// Encodes a batch as one or more unframed, uncompressed Bitz v2 payloads.
///
/// Batches that exceed a decoder budget are lazily compacted and split into
/// contiguous row ranges until each payload fits. Row order, selection, and
/// visible metadata are preserved. Splitting projects column indices instead
/// of rebuilding event rows.
/// A single event that cannot fit, or any non-resource encoding error, stops
/// the stream with an error. Earlier payloads may already have been yielded.
[[nodiscard]] auto encode_batches(Batch batch, EncodeOptions options = {})
  -> generator<Result<std::vector<std::byte>, std::string>>;

/// Decodes exactly one unframed, uncompressed Bitz v2 payload.
///
/// Rejects trailing bytes, malformed lengths, unknown type identifiers, and
/// excessive nesting. Decoding does not promise to retain the wire encoding.
[[nodiscard]] auto decode(std::span<std::byte const> payload,
                          DecodeLimits const& limits = default_decode_limits)
  -> Result<Batch, std::string>;

} // namespace tenzir::nova::bitz
