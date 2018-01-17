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

#ifndef VAST_DETAIL_VARBYTE_HPP
#define VAST_DETAIL_VARBYTE_HPP

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

namespace vast {
namespace detail {

/// The *variable byte* coding.
namespace varbyte {

/// Computes the size a given value will take in variable byte encoding.
template <class T>
constexpr std::enable_if_t<sizeof(T) == 8, size_t> size(T x) {
  return x >= (T(1) << 63) ? 10 :
         x >= (T(1) << 56) ? 9 :
         x >= (T(1) << 49) ? 8 :
         x >= (T(1) << 42) ? 7 :
         x >= (T(1) << 35) ? 6 :
         x >= (T(1) << 28) ? 5 :
         x >= (T(1) << 21) ? 4 :
         x >= (T(1) << 14) ? 3 :
         x >= (T(1) << 7) ? 2 : 1;
}

template <class T>
constexpr std::enable_if_t<sizeof(T) == 4, size_t> size(T x) {
  return x >= (T(1) << 28) ? 5 :
         x >= (T(1) << 21) ? 4 :
         x >= (T(1) << 14) ? 3 :
         x >= (T(1) << 7) ? 2 : 1;
}

template <class T>
constexpr std::enable_if_t<sizeof(T) == 2, size_t> size(T x) {
  return x >= (T(1) << 14) ? 3 :
         x >= (T(1) << 7) ? 2 : 1;
}

template <class T>
constexpr std::enable_if_t<sizeof(T) == 1, size_t> size(T x) {
  return x >= (T(1) << 7) ? 2 : 1;
}

/// Computes maximum number of bytes required to encode an integral type *T*.
template <class T>
constexpr size_t max_size() {
  return std::numeric_limits<T>::digits % 7 == 0
    ? std::numeric_limits<T>::digits / 7
    : std::numeric_limits<T>::digits / 7 + 1;
}

/// Encodes a value as variable byte sequence.
/// @tparam An integral type.
/// @param x The value to encode.
/// @param sink the output buffer to write into.
/// @returns The number of bytes written into *sink*.
template <class T>
std::enable_if_t<std::is_integral<T>{} && std::is_unsigned<T>{}, size_t>
encode(T x, void* sink) {
  auto out = reinterpret_cast<uint8_t*>(sink);
  while (x > 0x7f) {
    *out++ = (static_cast<uint8_t>(x) & 0x7f) | 0x80;
    x >>= 7;
  }
  *out++ = static_cast<uint8_t>(x) & 0x7f;
  return out - reinterpret_cast<uint8_t*>(sink);
}

/// Decodes a variable byte sequence into a value.
/// @tparam An integral type.
/// @param source The source buffer.
/// @param x The result of the decoding.
/// @returns The number of bytes read from *source*.
template <class T>
std::enable_if_t<std::is_integral<T>{} && std::is_unsigned<T>{}, size_t>
decode(T& x, void const* source) {
  auto in = reinterpret_cast<uint8_t const*>(source);
  size_t i = 0;
  uint8_t low7;
  x = 0;
  do {
    low7 = *in++;
    x |= static_cast<T>(low7 & 0x7f) << (7 * i);
    ++i;
  }
  while (low7 & 0x80);
  return i;
}

} // namespace varbyte
} // namespace detail
} // namespace vast

#endif
