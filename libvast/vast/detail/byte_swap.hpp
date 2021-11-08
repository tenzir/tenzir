//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/bit.hpp"

#include <cstdint>

namespace vast::detail {

/// Swaps the endianness of an unsigned integer.
/// @param x The value whose bytes to swap.
/// @returns The value with swapped byte order.
/// @see to_network_order to_host_order
inline uint8_t byte_swap(uint8_t x) {
  return x;
}

inline uint16_t byte_swap(uint16_t x) {
  return __builtin_bswap16(x);
}

inline uint32_t byte_swap(uint32_t x) {
  return __builtin_bswap32(x);
}

inline uint64_t byte_swap(uint64_t x) {
  return __builtin_bswap64(x);
}

/// Converts the bytes of an unsigned integer from host order to network order.
/// @param x The value to convert.
/// @returns The value with bytes in network order.
/// @see byte_swap to_host_order
template <class T>
T to_network_order(T x) {
  if constexpr (endian::native == endian::little)
    return byte_swap(x);
  else
    return x;
}

/// Converts the bytes of an unsigned integer from network order to host order.
/// @param x The value to convert.
/// @returns The value with bytes in host order.
/// @see byte_swap to_network_order
template <class T>
T to_host_order(T x) {
  return to_network_order(x);
}

/// Converts bytes from a given endian to a given endian.
/// @tparam From The endian of *x*.
/// @tparam To The endian of the result
/// @param x The value to convert.
/// @returns The value in with `To` endian.
/// @see endian byte_swap to_host_order to_network_order
template <endian From, endian To, class T>
T swap(T x) {
  if constexpr ((From == endian::little && To == endian::little)
                || (From == endian::big && To == endian::big))
    return x;
  else
    return byte_swap(x);
}

} // namespace vast::detail
