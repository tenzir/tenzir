//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/uniquely_represented.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <array>
#include <cstring>
#include <span>
#include <string>
#include <type_traits>

namespace tenzir {

/// An mac address.
class mac {
public:
  using byte_type = std::byte;
  using byte_array = std::array<byte_type, 6>;

  /// Default-constructs an invalid address.
  mac();

  /// Constructs a MAC address from 6 bytes in network byte order.
  /// @param bytes The 6 bytes representing the MAC address.
  constexpr explicit mac(byte_array bytes) : bytes_{bytes} {
  }

  /// Constructs a MAC address from a span of 6 bytes in network byte order.
  /// @param bytes The 6 bytes representing the MAC address.
  template <class Byte>
    requires(sizeof(Byte) == 1)
  explicit mac(std::span<Byte, 6> bytes) {
    std::memcpy(bytes_.data(), bytes.data(), 6);
  }

  /// Returns the *Organizationally Unique Identifier (OUI)* of the MAC address.
  auto oui() const -> std::span<const std::byte, 3>;

  /// Returns the *Organizationally Unique Identifier (OUI)* of the MAC address.
  auto nic() const -> std::span<const std::byte, 3>;

  /// Returns `true` *iff* the MAC address is universally administered.
  auto universal() const -> bool;

  /// Returns `true` *iff* the MAC address is a unicast address.
  auto unicast() const -> bool;

  friend auto inspect(auto& f, mac& x) {
    return f.object(x).pretty_name("mac").fields(f.field("bytes", x.bytes_));
  }

  template <class Byte = std::byte>
  friend auto as_bytes(const mac& x) -> std::span<const Byte, 6> {
    auto ptr = reinterpret_cast<const Byte*>(x.bytes_.data());
    return std::span<const Byte, 6>{ptr, 6};
  }

private:
  byte_array bytes_;
};

template <>
struct is_uniquely_represented<mac>
  : std::bool_constant<sizeof(mac) == sizeof(mac::byte_array)> {};

} // namespace tenzir

template <>
struct fmt::formatter<tenzir::mac> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const tenzir::mac& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{:02X}", fmt::join(as_bytes(x), "-"));
  }
};

namespace std {

template <>
struct hash<tenzir::mac> {
  auto operator()(const tenzir::mac& x) const -> size_t {
    return tenzir::hash(x);
  }
};

} // namespace std
