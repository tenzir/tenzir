//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <bit>
#include <cstdint>

namespace vast::detail {

/// Swaps the endianness of an unsigned integer.
/// @param x The value whose bytes to swap.
/// @returns The value with swapped byte order.
/// @see to_network_order to_host_order
inline auto byteswap(uint8_t x) -> uint8_t {
  return x;
}

inline auto byteswap(uint16_t x) -> uint16_t {
  return __builtin_bswap16(x);
}

inline auto byteswap(uint32_t x) -> uint32_t {
  return __builtin_bswap32(x);
}

inline auto byteswap(uint64_t x) -> uint64_t {
  return __builtin_bswap64(x);
}

/// Converts the bytes of an unsigned integer from host order to network order.
/// @param x The value to convert.
/// @returns The value with bytes in network order.
/// @see byteswap to_host_order
template <class T>
auto to_network_order(T x) -> T {
  if constexpr (std::endian::native == std::endian::little)
    return byteswap(x);
  else
    return x;
}

/// Converts the bytes of an unsigned integer from network order to host order.
/// @param x The value to convert.
/// @returns The value with bytes in host order.
/// @see byteswap to_network_order
template <class T>
auto to_host_order(T x) -> T {
  return to_network_order(x);
}

/// Converts bytes from a given endian to a given endian.
/// @tparam From The endian of *x*.
/// @tparam To The endian of the result
/// @param x The value to convert.
/// @returns The value in with `To` endian.
/// @see endian byte swap to_host_order to_network_order
template <std::endian From, std::endian To, class T>
auto swap(T x) -> T {
  if constexpr ((From == std::endian::little && To == std::endian::little)
                || (From == std::endian::big && To == std::endian::big))
    return x;
  else
    return byteswap(x);
}

} // namespace vast::detail
