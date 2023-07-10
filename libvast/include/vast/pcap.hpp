//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/type.hpp"

#include <cstdint>
#include <span>

/// PCAP utilities and data structures as defined in the IETF draft at
/// https://www.ietf.org/archive/id/draft-gharris-opsawg-pcap-01.html and
/// https://www.ietf.org/archive/id/draft-tuexen-opsawg-pcapng-05.html.
namespace vast::pcap {

/// The maximum snaplen as defined by MAXIMUM_SNAPLEN in libpcap (pcap-int.h).
constexpr uint32_t maximum_snaplen = 262'144;

/// File header magic number for microsecond timestamp precision.
constexpr uint32_t magic_number_1 = 0xa1b2c3d4;

/// File header magic number for nanosecond timestamp precision.
constexpr uint32_t magic_number_2 = 0xa1b23c4d;

/// The PCAP file header.
struct file_header {
  uint32_t magic_number;
  uint16_t major_version;
  uint16_t minor_version;
  uint32_t reserved1;
  uint32_t reserved2;
  uint32_t snaplen;
  uint32_t linktype;
} __attribute__((packed));

// The file header length is 24 octets.
static_assert(sizeof(file_header) == 24);

/// The packet header.
struct packet_header {
  uint32_t timestamp;
  uint32_t timestamp_fraction;
  uint32_t captured_packet_length;
  uint32_t original_packet_length;
} __attribute__((packed));

// The packet header length is 16 octets.
static_assert(sizeof(packet_header) == 16);

// PCAP files are written out with the system endianness, so we may have to
// swap bytes whenever the local endianness differs from the trace file. The
// magic number in the file helps identifying the endianness.

/// Swaps bytes in the file header.
auto byteswap(file_header hdr) -> file_header;

/// Swaps bytes in the packet header.
auto byteswap(packet_header hdr) -> packet_header;

/// Determines whether PCAP header values need byte swapping.
/// @returns `std::nullopt` on invalid magic and boolean otherwise.
auto need_byte_swap(uint32_t magic) -> std::optional<bool>;

/// A container for storing a single coming from the network. Header and data
/// lay next to each other on the wire.
struct packet_record {
  packet_header header;
  std::span<const std::byte> data;
};

/// Creates the `pcap.file_header` type.
/// @relates file_header
auto file_header_type() -> type;

/// Creates the `pcap.packet` type.
/// @relates packet_record
auto packet_record_type() -> type;

} // namespace vast::pcap
