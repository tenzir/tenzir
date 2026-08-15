//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/time.hpp"

#include <cstdint>

/// PCAPng utilities and data structures as defined in the IETF draft at
/// https://datatracker.ietf.org/doc/draft-tuexen-opsawg-pcapng/. Visit
/// https://pcapng.com/ for a high-level overview about the PCAPng format.
namespace tenzir::pcapng {

/// This field is the Block Type in a Section Header Block (SHB). It also serves
/// as magic number for the PCAPng file format.
constexpr uint32_t magic_number = 0x0a0d0d0a;

/// The byte-order magic in a Section Header Block.
constexpr uint32_t byte_order_magic = 0x1a2b3c4d;

constexpr uint32_t interface_description_block = 0x00000001;
constexpr uint32_t packet_block = 0x00000002;
constexpr uint32_t simple_packet_block = 0x00000003;
constexpr uint32_t enhanced_packet_block = 0x00000006;

constexpr uint32_t block_alignment = 4;
constexpr uint32_t block_min_size = 12;
constexpr uint32_t section_header_block_min_size = 28;
constexpr uint32_t interface_description_block_min_size = 20;
constexpr uint32_t packet_block_min_size = 32;
constexpr uint32_t simple_packet_block_min_size = 16;
constexpr uint32_t packet_data_offset = 28;
constexpr uint32_t simple_packet_data_offset = 12;

/// A zero snaplen indicates that the interface has no capture-length limit.
constexpr uint32_t unlimited_snaplen = 0;

constexpr uint16_t current_major_version = 1;
constexpr uint16_t current_minor_version = 0;
constexpr uint16_t compatible_minor_version = 2;

constexpr uint16_t end_of_options = 0;
constexpr uint16_t interface_timestamp_resolution_option = 9;
constexpr uint16_t interface_timestamp_offset_option = 14;

constexpr uint8_t binary_resolution_flag = 0x80;
constexpr uint8_t resolution_exponent_mask = 0x7f;
constexpr uint8_t default_timestamp_resolution = 6;
constexpr uint8_t nanosecond_timestamp_resolution = 9;

struct TimestampFormat {
  uint8_t resolution = default_timestamp_resolution;
  int64_t offset_seconds = 0;
};

/// Returns the size rounded up to the next PCAPng block boundary.
constexpr auto padded_size(uint32_t size) -> uint64_t {
  return (uint64_t{size} + block_alignment - 1)
         & ~(uint64_t{block_alignment} - 1);
}

/// Decodes a raw PCAPng timestamp.
///
/// Returns `None` if the timestamp is outside Tenzir's time range.
auto decode_timestamp(uint64_t value, TimestampFormat format) -> Option<time>;

/// Encodes a Tenzir timestamp for a PCAPng interface.
///
/// Precision finer than the requested resolution is discarded. Returns `None`
/// if the timestamp precedes the interface epoch or exceeds the PCAPng range.
auto encode_timestamp(time value, TimestampFormat format) -> Option<uint64_t>;

} // namespace tenzir::pcapng
