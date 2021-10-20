//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/hash.hpp"
#include "vast/concept/hashable/legacy_hash.hpp"
#include "vast/concept/hashable/uniquely_represented.hpp"
#include "vast/detail/bit.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/operators.hpp"

#include <array>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <type_traits>

namespace vast {

/// An IP address.
class address : detail::totally_ordered<address>, detail::bitwise<address> {
public:
  using byte_type = uint8_t;
  using byte_array = std::array<byte_type, 16>;

  /// Top 96 bits of v4-mapped-addr.
  static constexpr std::array<byte_type, 12> v4_mapped_prefix
    = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}};

  /// Address family.
  enum family { ipv4, ipv6 };

  /// Constructs an IPv4 address from raw bytes in network byte order.
  /// @param bytes A pointer to 4 bytes.
  /// @returns An IPv4 address constructed from *bytes*.
  template <class Byte>
    requires(sizeof(Byte) == 1)
  static address v4(std::span<Byte, 4> bytes) {
    address result;
    std::memcpy(&result.bytes_[0], v4_mapped_prefix.data(), 12);
    std::memcpy(&result.bytes_[12], bytes.data(), 4);
    return result;
  }

  /// Constructs an IPv4 address from a 32-bit unsigned integer.
  /// @tparam Endian the address byte order.
  /// @param bytes The 32-bit integer representing an IPv4 address.
  /// @returns The IP address.
  template <detail::endian Endian = detail::endian::native>
  static address v4(uint32_t bytes) {
    if constexpr (Endian == detail::endian::little)
      bytes = detail::byte_swap(bytes);
    auto ptr = reinterpret_cast<const byte_type*>(&bytes);
    return v4(std::span<const byte_type, 4>{ptr, 4});
  }

  /// Constructs an IPv6 address from 16 raw bytes.
  /// @param bytes A span of 16 bytes.
  /// @returns An IPv6 address constructed from *bytes*.
  template <class Byte>
    requires(sizeof(Byte) == 1)
  static address v6(std::span<Byte, 16> bytes) {
    address result;
    std::memcpy(result.bytes_.data(), bytes.data(), 16);
    return result;
  }

  template <detail::endian Endian = detail::endian::native>
  static address v6(std::span<uint32_t, 4> bytes) {
    address result;
    std::memcpy(result.bytes_.data(), bytes.data(), 16);
    if constexpr (Endian == detail::endian::little) {
      auto ptr = reinterpret_cast<uint32_t*>(result.bytes_.data());
      auto span = std::span<uint32_t, 4>{ptr, 4};
      for (auto& block : span)
        block = detail::byte_swap(block);
    }
    return result;
  }

  /// Default-constructs an (invalid) address.
  constexpr address() {
    bytes_.fill(0);
  }

  /// Constructs an IP address from 16 bytes in network byte order.
  /// @param bytes The 16 bytes representing the IP address.
  constexpr explicit address(byte_array bytes) : bytes_{bytes} {
  }

  template <class Byte>
    requires(sizeof(Byte) == 1)
  explicit address(std::span<Byte, 16> bytes) {
    std::memcpy(bytes_.data(), bytes.data(), 16);
  }

  /// Determines whether the address is IPv4.
  /// @returns @c true iff the address is an IPv4 address.
  [[nodiscard]] bool is_v4() const;

  /// Determines whether the address is IPv4.
  /// @returns `true` iff the address is an IPv4 address.
  [[nodiscard]] bool is_v6() const;

  /// Determines whether the address is an IPv4 loopback address.
  /// @returns `true` if the address is v4 and its first byte has the
  /// value 127.
  [[nodiscard]] bool is_loopback() const;

  /// Determines whether the address is an IPv4 broadcast address.
  /// @returns `true` if the address is v4 and has the value 255.255.255.255.
  [[nodiscard]] bool is_broadcast() const;

  /// Determines whether the address is a multicast address. For v4
  /// addresses, this means the first byte equals to 224. For v6 addresses,
  /// this means the first bytes equals 255.
  /// @returns `true` if the address is a multicast address.
  [[nodiscard]] bool is_multicast() const;

  /// Masks out lower bits of the address.
  /// @param top_bits_to_keep The number of bits *not* to mask out,
  ///                         counting from the highest order bit. The value is
  ///                         always interpreted relative to the IPv6 bit
  ///                         width, even if the address is IPv4. That means if
  ///                         we compute 192.168.1.2/16, we need to pass in
  ///                         112 (i.e., 96 + 16). The value must be in the
  ///                         range from 0 to 128.
  /// @returns `true` on success.
  bool mask(unsigned top_bits_to_keep);

  /// AND's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator&=(const address& other);

  /// OR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator|=(const address& other);

  /// XOR's another address to this instance.
  /// @param other The other address.
  /// @returns A reference to `*this`.
  address& operator^=(const address& other);

  /// Compares the top-k bits of this address with another one.
  /// @param other The other address.
  /// @param k The number of bits to compare, starting from the top.
  /// @returns `true` if the first *k* bits of both addresses are equal
  /// @pre `k > 0 && k <= 128`
  [[nodiscard]] bool compare(const address& other, size_t k) const;

  explicit constexpr operator byte_array() const {
    return bytes_;
  }

  friend bool operator==(const address& x, const address& y);
  friend bool operator<(const address& x, const address& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, address& x) {
    return f(x.bytes_);
  }

  template <class Byte = std::byte>
  friend std::span<const Byte, 16> as_bytes(const address& x) {
    auto ptr = reinterpret_cast<const Byte*>(x.bytes_.data());
    return std::span<const Byte, 16>{ptr, 16};
  }

private:
  byte_array bytes_;
};

template <>
struct is_uniquely_represented<address>
  : std::bool_constant<sizeof(address) == sizeof(address::byte_array)> {};

// TODO: remove after we have introduced versioned flatbuffer state and all our
// users have no more lingering persistent data. This hash_append overload
// brings back the old hashing behavior that hashes a different number of bytes
// based on the IP address version, at the cost of an extra branch. The new
// version unconditionally hashes all 16 bytes.
inline auto hash_append(legacy_hash& h, const address& x) {
  if (x.is_v4())
    hash_append(h, as_bytes(x).subspan<12, 4>());
  else
    hash_append(h, as_bytes(x).subspan<0, 16>());
}

} // namespace vast

namespace std {

template <>
struct hash<vast::address> {
  size_t operator()(const vast::address& x) const {
    return vast::hash(x);
  }
};

} // namespace std
