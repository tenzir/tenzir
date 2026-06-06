//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ip.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/concept/printable/tenzir/ip.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"

#include <arpa/inet.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <utility>

namespace tenzir {

namespace {

auto all_equal(std::span<uint8_t const> bytes, uint8_t value) -> bool {
  return std::ranges::all_of(bytes, [value](auto byte) {
    return byte == value;
  });
}

auto all_zero(std::span<uint8_t const> bytes) -> bool {
  return all_equal(bytes, 0);
}

} // namespace

auto ip::is_v4() const -> bool {
  return std::memcmp(bytes_.data(), v4_mapped_prefix.data(),
                     v4_mapped_prefix.size())
         == 0;
}

auto ip::is_v6() const -> bool {
  return not is_v4();
}

auto ip::is_loopback() const -> bool {
  if (is_v4()) {
    return bytes_[12] == 127;
  } else {
    return all_zero(std::span{bytes_}.first<15>()) and bytes_[15] == 1;
  }
}

auto ip::is_broadcast() const -> bool {
  return is_v4() and all_equal(std::span{bytes_}.subspan<12, 4>(), 0xff);
}

auto ip::is_multicast() const -> bool {
  return is_v4() ? (bytes_[12] >= 224 and bytes_[12] <= 239)
                 : bytes_[0] == 0xff;
}

auto ip::is_private() const -> bool {
  if (is_v4()) {
    // IPv4 private addresses:
    // 10.0.0.0/8 (10.0.0.0 - 10.255.255.255)
    // 172.16.0.0/12 (172.16.0.0 - 172.31.255.255)
    // 192.168.0.0/16 (192.168.0.0 - 192.168.255.255)
    // Note: 169.254.0.0/16 (link-local) is NOT included here
    auto first = bytes_[12];
    auto second = bytes_[13];
    return (first == 10) or (first == 172 and second >= 16 and second <= 31)
           or (first == 192 and second == 168);
  } else {
    // IPv6 private addresses:
    // fc00::/7 - Unique Local Addresses (ULA)
    // Note: fe80::/10 (link-local) is NOT included here
    return bytes_[0] >= 0xfc and bytes_[0] <= 0xfd;
  }
}

auto ip::is_global() const -> bool {
  // A global address is one that is not:
  // - loopback
  // - private
  // - link-local
  // - multicast
  // - broadcast (IPv4 only)
  // - unspecified (0.0.0.0 or ::)

  // Check for unspecified address
  if (is_v4()) {
    // For IPv4, check if the last 4 bytes are all zero (0.0.0.0)
    if (all_zero(std::span{bytes_}.subspan<12, 4>())) {
      return false;
    }
  } else {
    // For IPv6, check if all bytes are zero (::)
    if (all_zero(std::span{bytes_})) {
      return false;
    }
  }

  return not is_loopback() and not is_private() and not is_link_local()
         and not is_multicast() and not is_broadcast();
}

auto ip::is_link_local() const -> bool {
  if (is_v4()) {
    // IPv4 link-local: 169.254.0.0/16
    return bytes_[12] == 169 and bytes_[13] == 254;
  } else {
    // IPv6 link-local: fe80::/10
    return bytes_[0] == 0xfe and ((bytes_[1] & 0xc0) == 0x80);
  }
}

auto ip::type() const -> ip_address_class {
  // Check for unspecified address first (0.0.0.0 or ::)
  if (is_v4()) {
    // For IPv4, check if the last 4 bytes are all zero (0.0.0.0)
    if (all_zero(std::span{bytes_}.subspan<12, 4>())) {
      return ip_address_class::unspecified;
    }
  } else {
    // For IPv6, check if all bytes are zero (::)
    if (all_zero(std::span{bytes_})) {
      return ip_address_class::unspecified;
    }
  }

  // Check in order of specificity
  if (is_loopback()) {
    return ip_address_class::loopback;
  }
  if (is_link_local()) {
    return ip_address_class::link_local;
  }
  if (is_multicast()) {
    return ip_address_class::multicast;
  }
  if (is_broadcast()) {
    return ip_address_class::broadcast;
  }
  if (is_private()) {
    return ip_address_class::private_;
  }
  // Everything else is global
  return ip_address_class::global;
}

auto ip::mask(unsigned top_bits_to_keep) -> bool {
  if (top_bits_to_keep > 128) {
    return false;
  }
  auto byte_index = top_bits_to_keep / 8;
  auto bits_to_keep = top_bits_to_keep % 8;
  if (bits_to_keep != 0) {
    auto mask = static_cast<uint8_t>(0xff << (8 - bits_to_keep));
    bytes_[byte_index] &= mask;
    ++byte_index;
  }
  std::ranges::fill(std::span{bytes_}.subspan(byte_index), 0);
  return true;
}

auto ip::operator&=(const ip& other) -> ip& {
  for (auto i = 0u; i < 16u; ++i) {
    bytes_[i] &= other.bytes_[i];
  }
  return *this;
}

auto ip::operator|=(const ip& other) -> ip& {
  for (auto i = 0u; i < 16u; ++i) {
    bytes_[i] |= other.bytes_[i];
  }
  return *this;
}

auto ip::operator^=(const ip& other) -> ip& {
  if (is_v4() or other.is_v4()) {
    for (auto i = 12u; i < 16u; ++i) {
      bytes_[i] ^= other.bytes_[i];
    }
  } else {
    for (auto i = 0u; i < 16u; ++i) {
      bytes_[i] ^= other.bytes_[i];
    }
  }
  return *this;
}

auto ip::compare(const ip& other, size_t k) const -> bool {
  TENZIR_ASSERT(k <= 128);
  if (k == 0) { // trivially true
    return true;
  }
  auto x = bytes_.data();
  auto y = other.bytes_.data();
  for (; k > 8; k -= 8) {
    if (*x++ != *y++) {
      return false;
    }
  }
  auto mask = static_cast<uint8_t>(0xff << (8 - k));
  return (*x & mask) == (*y & mask);
}

} // namespace tenzir

auto fmt::formatter<tenzir::ip>::format(const tenzir::ip& value,
                                        format_context& ctx) const
  -> format_context::iterator {
  auto buffer = std::array<char, INET6_ADDRSTRLEN>{};
  buffer.fill(0);
  auto bytes = as_bytes(value);
  if (value.is_v4()) {
    const auto* result
      = inet_ntop(AF_INET, &bytes[12], buffer.data(), INET_ADDRSTRLEN);
    TENZIR_ASSERT(result != nullptr);
  } else {
    const auto* result
      = inet_ntop(AF_INET6, bytes.data(), buffer.data(), INET6_ADDRSTRLEN);
    TENZIR_ASSERT(result != nullptr);
  }
  auto str = std::string_view{buffer.data()};
  return formatter<string_view>::format(str, ctx);
}
