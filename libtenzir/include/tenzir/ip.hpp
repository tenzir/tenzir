//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/debug_writer.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/legacy_hash.hpp"
#include "tenzir/hash/uniquely_hashable.hpp"
#include "tenzir/hash/uniquely_represented.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <type_traits>

namespace tenzir {

/// An IP address.
class ip : detail::totally_ordered<ip>, detail::bitwise<ip> {
public:
  using byte_type = uint8_t;
  using byte_array = std::array<byte_type, 16>;

  /// Top 96 bits of v4-mapped-addr.
  static constexpr std::array<byte_type, 12> v4_mapped_prefix
    = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}};

  static inline constexpr auto pseudonymization_seed_array_size = size_t{32};

  /// Address family.
  enum family { ipv4, ipv6 };

  /// Constructs an IPv4 address from raw bytes in network byte order.
  /// @param bytes A pointer to 4 bytes.
  /// @returns An IPv4 address constructed from *bytes*.
  template <class Byte>
    requires(sizeof(Byte) == 1)
  static auto v4(std::span<Byte, 4> bytes) -> ip {
    ip result;
    std::memcpy(&result.bytes_[0], v4_mapped_prefix.data(), 12);
    std::memcpy(&result.bytes_[12], bytes.data(), 4);
    return result;
  }

  /// Constructs an IPv4 address from a 32-bit unsigned integer.
  /// @tparam Endian the address byte order.
  /// @param bytes The 32-bit integer representing an IPv4 address.
  /// @returns The IP address.
  template <std::endian Endian = std::endian::native>
  static auto v4(uint32_t bytes) -> ip {
    if constexpr (Endian == std::endian::little) {
      bytes = detail::byteswap(bytes);
    }
    auto ptr = reinterpret_cast<const byte_type*>(&bytes);
    return v4(std::span<const byte_type, 4>{ptr, 4});
  }

  /// Constructs an IPv6 address from 16 raw bytes.
  /// @param bytes A span of 16 bytes.
  /// @returns An IPv6 address constructed from *bytes*.
  template <class Byte>
    requires(sizeof(Byte) == 1)
  static auto v6(std::span<Byte, 16> bytes) -> ip {
    ip result;
    std::memcpy(result.bytes_.data(), bytes.data(), 16);
    return result;
  }

  template <std::endian Endian = std::endian::native>
  static auto v6(std::span<uint32_t, 4> bytes) -> ip {
    ip result;
    std::memcpy(result.bytes_.data(), bytes.data(), 16);
    if constexpr (Endian == std::endian::little) {
      auto ptr = reinterpret_cast<uint32_t*>(result.bytes_.data());
      auto span = std::span<uint32_t, 4>{ptr, 4};
      for (auto& block : span) {
        block = detail::byteswap(block);
      }
    }
    return result;
  }

  /// Construct a pseudonymized address using the Crypto-PAn algorithm.
  /// @param original The address to be pseudonymized.
  /// @param seed 256-bit seed for the cipher and padding.
  /// @returns A copy of the `original` address with pseudonymized bytes.
  static auto pseudonymize(
    const ip& original,
    const std::array<byte_type, pseudonymization_seed_array_size>& seed) -> ip;

  /// Default-constructs an (invalid) address.
  constexpr ip() {
    bytes_.fill(0);
  }

  /// Constructs an IP address from 16 bytes in network byte order.
  /// @param bytes The 16 bytes representing the IP address.
  constexpr explicit ip(byte_array bytes) : bytes_{bytes} {
  }

  template <class Byte>
    requires(sizeof(Byte) == 1)
  explicit ip(std::span<Byte, 16> bytes) {
    std::memcpy(bytes_.data(), bytes.data(), 16);
  }

  /// Determines whether the address is IPv4.
  /// @returns @c true iff the address is an IPv4 address.
  [[nodiscard]] auto is_v4() const -> bool;

  /// Determines whether the address is IPv4.
  /// @returns `true` iff the address is an IPv4 address.
  [[nodiscard]] auto is_v6() const -> bool;

  /// Determines whether the address is an IPv4 loopback address.
  /// @returns `true` if the address is v4 and its first byte has the
  /// value 127.
  [[nodiscard]] auto is_loopback() const -> bool;

  /// Determines whether the address is an IPv4 broadcast address.
  /// @returns `true` if the address is v4 and has the value 255.255.255.255.
  [[nodiscard]] auto is_broadcast() const -> bool;

  /// Determines whether the address is a multicast address. For v4
  /// addresses, this means the first byte equals to 224. For v6 addresses,
  /// this means the first bytes equals 255.
  /// @returns `true` if the address is a multicast address.
  [[nodiscard]] auto is_multicast() const -> bool;

  /// Masks out lower bits of the address.
  /// @param top_bits_to_keep The number of bits *not* to mask out,
  ///                         counting from the highest order bit. The value is
  ///                         always interpreted relative to the IPv6 bit
  ///                         width, even if the address is IPv4. That means if
  ///                         we compute 192.168.1.2/16, we need to pass in
  ///                         112 (i.e., 96 + 16). The value must be in the
  ///                         range from 0 to 128.
  /// @returns `true` on success.
  auto mask(unsigned top_bits_to_keep) -> bool;

  /// AND's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  auto operator&=(const ip& other) -> ip&;

  /// OR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  auto operator|=(const ip& other) -> ip&;

  /// XOR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  auto operator^=(const ip& other) -> ip&;

  /// Compares the top-k bits of this address with another one.
  /// @param other The other address.
  /// @param k The number of bits to compare, starting from the top.
  /// @returns `true` if the first *k* bits of both addresses are equal
  /// @pre `k > 0 && k <= 128`
  [[nodiscard]] auto compare(const ip& other, size_t k) const -> bool;

  explicit constexpr operator byte_array() const {
    return bytes_;
  }

  friend auto operator==(const ip& x, const ip& y) -> bool;
  friend auto operator<(const ip& x, const ip& y) -> bool;

  template <class Inspector>
  friend auto inspect(Inspector& f, ip& x) {
    if (auto g = as_debug_writer(f)) {
      return g->fmt_value("{}", x);
    }
    return f.apply(x.bytes_);
  }

  template <class Byte = std::byte>
  friend auto as_bytes(const ip& x) -> std::span<const Byte, 16> {
    auto ptr = reinterpret_cast<const Byte*>(x.bytes_.data());
    return std::span<const Byte, 16>{ptr, 16};
  }

private:
  byte_array bytes_;
};

template <>
struct is_uniquely_represented<ip>
  : std::bool_constant<sizeof(ip) == sizeof(ip::byte_array)> {};

// TODO: this specialization disables oneshot hashing for addresses to force
// hashing of addresses via hash_append when using the legacy hash function.
// Remove this, along with the hash_append overload, after we have introduced
// versioned flatbuffer state and all our users have no more lingering
// persistent data.

template <>
struct is_uniquely_hashable<ip, legacy_hash> : std::false_type {};

inline auto hash_append(legacy_hash& h, const ip& x) {
  if (x.is_v4()) {
    hash_append(h, as_bytes(x).subspan<12, 4>());
  } else {
    hash_append(h, as_bytes(x).subspan<0, 16>());
  }
}

} // namespace tenzir

template <>
struct fmt::formatter<tenzir::ip> : formatter<string_view> {
  auto format(const tenzir::ip& x, format_context& ctx) const
    -> format_context::iterator;
};

namespace std {

template <>
struct hash<tenzir::ip> {
  auto operator()(const tenzir::ip& x) const -> size_t {
    return tenzir::hash(x);
  }
};

} // namespace std
