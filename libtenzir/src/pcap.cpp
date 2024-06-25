//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pcap.hpp"

#include "tenzir/detail/byteswap.hpp"

namespace tenzir::pcap {

auto as_bytes(const file_header& header)
  -> std::span<const std::byte, sizeof(file_header)> {
  const auto* ptr = reinterpret_cast<const std::byte*>(&header);
  return std::span<const std::byte, sizeof(file_header)>{ptr,
                                                         sizeof(file_header)};
}

auto as_writeable_bytes(file_header& header)
  -> std::span<std::byte, sizeof(file_header)> {
  auto* ptr = reinterpret_cast<std::byte*>(&header);
  return std::span<std::byte, sizeof(file_header)>{ptr, sizeof(file_header)};
}

auto as_bytes(const packet_header& header)
  -> std::span<const std::byte, sizeof(packet_header)> {
  const auto* ptr = reinterpret_cast<const std::byte*>(&header);
  return std::span<const std::byte, sizeof(packet_header)>{
    ptr, sizeof(packet_header)};
}

auto as_writeable_bytes(packet_header& header)
  -> std::span<std::byte, sizeof(packet_header)> {
  auto* ptr = reinterpret_cast<std::byte*>(&header);
  return std::span<std::byte, sizeof(packet_header)>{ptr,
                                                     sizeof(packet_header)};
}

auto is_file_header(const packet_header& header) -> bool {
  // Here they are two headers side by side:
  //
  //                FILE HEADER                      PACKET HEADER
  //
  //     ┌───────────────────────────────┐  ┌───────────────────────────────┐
  //     │         MAGIC NUMBER          │  │           TIMESTAMP           │
  //     ├───────────────┬───────────────┤  ├───────────────────────────────┤
  //     │ MAJOR VERSION │ MINOR VERSION │  │       TIMESTAMP FRACTION      │
  //     ├───────────────┴───────────────┤  ├───────────────────────────────┤
  //     │           RESERVED            │  │     CAPTURED PACKET LENGTH    │
  //     ├───────────────────────────────┤  ├───────────────────────────────┤
  //     │           RESERVED            │  │     ORIGINAL PACKET LENGTH    │
  //     ├───────────────────────────────┤  └───────────────────────────────┘
  //                  SNAPLEN
  //     ├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤
  //                 LINKTYPE
  //     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
  //
  auto is_reserved
    = header.captured_packet_length == 0 && header.original_packet_length == 0;
  if (not is_reserved)
    return false;
  // In theory, checking for zeroed out reserved fields should be sufficient.
  // But we don't all PCAP generating tools, so do a few extra checks.
  auto is_magic = header.timestamp == pcap::magic_number_1
                  || header.timestamp == pcap::magic_number_2;
  if (not is_magic)
    return false;
  // We're actually stopping here for now, even though we could go deeper. The
  // base rate is too low for this.
  return true;
  // What could go wrong if we didn't do the next checks? The literal magic
  // values would be UNIX timestamps equivalent to Dec 19, 2055. At this point
  // AGI will have killed us all. If we got (real or simulated) packets from
  // that very second in the future, we deem it next to impossible that the
  // fractional timestamp accidentally matched the PCAP version.
  auto major_version = header.timestamp_fraction >> 16;
  auto minor_version = header.timestamp_fraction & 0xffff;
  if (need_byte_swap(header.timestamp)) {
    major_version = detail::byteswap(major_version);
    minor_version = detail::byteswap(minor_version);
  }
  return major_version == 4 && minor_version == 2;
}

auto byteswap(file_header hdr) -> file_header {
  auto result = file_header{};
  result.magic_number = detail::byteswap(hdr.magic_number);
  result.major_version = detail::byteswap(hdr.major_version);
  result.minor_version = detail::byteswap(hdr.minor_version);
  result.reserved1 = detail::byteswap(hdr.reserved1);
  result.reserved2 = detail::byteswap(hdr.reserved2);
  result.snaplen = detail::byteswap(hdr.snaplen);
  result.linktype = detail::byteswap(hdr.linktype);
  return result;
}

auto byteswap(packet_header hdr) -> packet_header {
  auto result = packet_header{};
  result.timestamp = detail::byteswap(hdr.timestamp);
  result.timestamp_fraction = detail::byteswap(hdr.timestamp_fraction);
  result.captured_packet_length = detail::byteswap(hdr.captured_packet_length);
  result.original_packet_length = detail::byteswap(hdr.original_packet_length);
  return result;
}

auto need_byte_swap(uint32_t magic) -> std::optional<bool> {
  auto swapped = detail::byteswap(magic);
  if (magic == magic_number_1 || magic == magic_number_2)
    return false;
  if (swapped == magic_number_1 || swapped == magic_number_2)
    return true;
  return std::nullopt;
}

auto file_header_type() -> type {
  return type{
    "pcap.file_header",
    record_type{
      {"magic_number", uint64_type{}},  // uint32
      {"major_version", uint64_type{}}, // uint32
      {"minor_version", uint64_type{}}, // uint32
      {"reserved1", uint64_type{}},     // uint32
      {"reserved2", uint64_type{}},     // uint32
      {"snaplen", uint64_type{}},       // uint32
      {"linktype", uint64_type{}},      // uint16
    },
  };
}

auto packet_record_type() -> type {
  return type{
    "pcap.packet",
    record_type{
      {"linktype", uint64_type{}}, // uint16 would suffice
      {"timestamp", time_type{}},
      {"captured_packet_length", uint64_type{}},
      {"original_packet_length", uint64_type{}},
      {"data", type{blob_type{}, {{"skip"}}}},
    },
  };
}

} // namespace tenzir::pcap
