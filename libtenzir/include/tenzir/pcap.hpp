//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/type.hpp"

#include <cstdint>
#include <span>
#include <string_view>

/// PCAP utilities and data structures as defined in the IETF draft at
/// https://www.ietf.org/archive/id/draft-gharris-opsawg-pcap-01.html and
/// https://www.ietf.org/archive/id/draft-tuexen-opsawg-pcapng-05.html.
namespace tenzir::pcap {

/// The PCAP content type.
constexpr std::string_view content_type = "application/vnd.tcpdump.pcap";

/// The maximum snaplen as defined by MAXIMUM_SNAPLEN in libpcap (pcap-int.h).
constexpr uint32_t maximum_snaplen = 262'144;

/// File header magic number for microsecond timestamp precision.
constexpr uint32_t magic_number_1 = 0xa1b2c3d4;

/// File header magic number for nanosecond timestamp precision.
constexpr uint32_t magic_number_2 = 0xa1b23c4d;

/// The PCAP file header.
struct FileHeader {
  uint32_t magic_number;
  uint16_t major_version;
  uint16_t minor_version;
  uint32_t reserved1;
  uint32_t reserved2;
  uint32_t snaplen;
  uint32_t linktype;
} __attribute__((packed));

// The file header length is 24 octets.
static_assert(sizeof(FileHeader) == 24);

auto as_bytes(FileHeader const& header)
  -> std::span<std::byte const, sizeof(FileHeader)>;

auto as_writeable_bytes(FileHeader& header)
  -> std::span<std::byte, sizeof(FileHeader)>;

/// The packet header.
struct PacketHeader {
  uint32_t timestamp;
  uint32_t timestamp_fraction;
  uint32_t captured_packet_length;
  uint32_t original_packet_length;
} __attribute__((packed));

// The packet header length is 16 octets.
static_assert(sizeof(PacketHeader) == 16);

auto as_bytes(PacketHeader const& header)
  -> std::span<std::byte const, sizeof(PacketHeader)>;

auto as_writeable_bytes(PacketHeader& header)
  -> std::span<std::byte, sizeof(PacketHeader)>;

// Checks whether a packet header is actually a packet header. This is a
// heuristic based on the binary shape of the header, not a standard-compliant
// check. However, it works robustly in practice.
auto is_file_header(PacketHeader const& header) -> bool;

// PCAP files are written out with the system endianness, so we may have to
// swap bytes whenever the local endianness differs from the trace file. The
// magic number in the file helps identifying the endianness.

/// Swaps bytes in the file header.
auto byteswap(FileHeader header) -> FileHeader;

/// Swaps bytes in the packet header.
auto byteswap(PacketHeader header) -> PacketHeader;

/// Determines whether PCAP header values need byte swapping.
///
/// Returns `None` for an invalid magic number.
auto need_byte_swap(uint32_t magic) -> Option<bool>;

/// A container for storing a single packet. Header and data lie next to each
/// other on the wire.
struct PacketRecord {
  PacketHeader header;
  std::span<std::byte const> data;
};

/// Creates the `pcap.file_header` type.
/// @relates FileHeader
auto file_header_type() -> type;

/// Creates the `pcap.packet` type.
/// @relates PacketRecord
auto packet_record_type() -> type;

} // namespace tenzir::pcap
