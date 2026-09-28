//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

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
/// The encoding is canonical and recursively columnar. Structural values are
/// little-endian. Fixed-width scalar array bodies use the configured byte
/// order, which defaults to the host's native order. Physical array encodings
/// are deliberately not retained. Visibility is propagated through nested
/// arrays, and inaccessible positions receive placeholders without changing
/// physical row or child-array positions. The
/// encoder first calculates the exact payload size, then allocates the result
/// once and writes directly into it.
[[nodiscard]] auto encode(Batch const& batch, EncodeOptions const& options = {})
  -> Result<std::vector<std::byte>, std::string>;

/// Decodes exactly one unframed, uncompressed Bitz v2 payload.
///
/// Rejects trailing bytes, malformed lengths, unknown type identifiers, and
/// excessive nesting. The returned arrays use their canonical builder-chosen
/// physical representations.
[[nodiscard]] auto decode(std::span<std::byte const> payload,
                          DecodeLimits const& limits = default_decode_limits)
  -> Result<Batch, std::string>;

} // namespace tenzir::nova::bitz
