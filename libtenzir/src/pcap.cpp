//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pcap.hpp"

#include "tenzir/detail/byteswap.hpp"

namespace tenzir::pcap {

auto as_bytes(FileHeader const& header)
  -> std::span<std::byte const, sizeof(FileHeader)> {
  auto const* ptr = reinterpret_cast<std::byte const*>(&header);
  return std::span<std::byte const, sizeof(FileHeader)>{ptr,
                                                        sizeof(FileHeader)};
}

auto as_writeable_bytes(FileHeader& header)
  -> std::span<std::byte, sizeof(FileHeader)> {
  auto* ptr = reinterpret_cast<std::byte*>(&header);
  return std::span<std::byte, sizeof(FileHeader)>{ptr, sizeof(FileHeader)};
}

auto as_bytes(PacketHeader const& header)
  -> std::span<std::byte const, sizeof(PacketHeader)> {
  auto const* ptr = reinterpret_cast<std::byte const*>(&header);
  return std::span<std::byte const, sizeof(PacketHeader)>{ptr,
                                                          sizeof(PacketHeader)};
}

auto as_writeable_bytes(PacketHeader& header)
  -> std::span<std::byte, sizeof(PacketHeader)> {
  auto* ptr = reinterpret_cast<std::byte*>(&header);
  return std::span<std::byte, sizeof(PacketHeader)>{ptr, sizeof(PacketHeader)};
}

auto is_file_header(PacketHeader const& header) -> bool {
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
    = header.captured_packet_length == 0 and header.original_packet_length == 0;
  if (not is_reserved) {
    return false;
  }
  // In theory, checking for zeroed out reserved fields should be sufficient.
  // But we don't trust all PCAP generating tools, so do a few extra checks.
  // Accept both host-order and swapped raw magic values so concatenated traces
  // with mixed endianness still recognize the next file header.
  if (not need_byte_swap(header.timestamp)) {
    return false;
  }
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
  return major_version == 4 and minor_version == 2;
}

auto byteswap(FileHeader header) -> FileHeader {
  auto result = FileHeader{};
  result.magic_number = detail::byteswap(header.magic_number);
  result.major_version = detail::byteswap(header.major_version);
  result.minor_version = detail::byteswap(header.minor_version);
  result.reserved1 = detail::byteswap(header.reserved1);
  result.reserved2 = detail::byteswap(header.reserved2);
  result.snaplen = detail::byteswap(header.snaplen);
  result.linktype = detail::byteswap(header.linktype);
  return result;
}

auto byteswap(PacketHeader header) -> PacketHeader {
  auto result = PacketHeader{};
  result.timestamp = detail::byteswap(header.timestamp);
  result.timestamp_fraction = detail::byteswap(header.timestamp_fraction);
  result.captured_packet_length
    = detail::byteswap(header.captured_packet_length);
  result.original_packet_length
    = detail::byteswap(header.original_packet_length);
  return result;
}

auto need_byte_swap(uint32_t magic) -> Option<bool> {
  auto swapped = detail::byteswap(magic);
  if (magic == magic_number_1 or magic == magic_number_2) {
    return false;
  }
  if (swapped == magic_number_1 or swapped == magic_number_2) {
    return true;
  }
  return None{};
}

} // namespace tenzir::pcap
