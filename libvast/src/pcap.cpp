//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pcap.hpp"

#include "vast/detail/byteswap.hpp"

namespace vast::pcap {

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
  const auto timestamp_type = type{"timestamp", time_type{}};
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
  const auto timestamp_type = type{"timestamp", time_type{}};
  return type{
    "pcap.packet",
    record_type{
      {"linktype", uint64_type{}}, // uint16 would suffice
      {"timestamp", timestamp_type},
      {"captured_packet_length", uint64_type{}},
      {"original_packet_length", uint64_type{}},
      {"data", type{string_type{}, {{"skip"}}}},
    },
  };
}

} // namespace vast::pcap
