/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_DETAIL_BYTE_SWAP_HPP
#define VAST_DETAIL_BYTE_SWAP_HPP

#include "vast/detail/endian.hpp"

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
/// @see endianness byte_swap to_host_order
template <class T>
T to_network_order(T x) {
#if defined(VAST_LITTLE_ENDIAN)
  return byte_swap(x);
#elif defined(VAST_BIG_ENDIAN)
  return x;
#endif
}

/// Converts the bytes of an unsigned integer from network order to host order.
/// @param x The value to convert.
/// @returns The value with bytes in host order.
/// @see endianness byte_swap to_network_order
template <class T>
T to_host_order(T x) {
  return to_network_order(x);
}

} // namespace vast::detail

#endif
