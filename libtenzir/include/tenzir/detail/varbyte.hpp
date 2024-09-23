//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

/// The *variable byte* coding.
namespace tenzir::detail::varbyte {

/// Computes the size a given value will take in variable byte encoding.
template <class T>
constexpr size_t size(T x) noexcept {
  if constexpr (sizeof(T) == 8) {
    return x >= (T(1) << 63)   ? 10
           : x >= (T(1) << 56) ? 9
           : x >= (T(1) << 49) ? 8
           : x >= (T(1) << 42) ? 7
           : x >= (T(1) << 35) ? 6
           : x >= (T(1) << 28) ? 5
           : x >= (T(1) << 21) ? 4
           : x >= (T(1) << 14) ? 3
           : x >= (T(1) << 7)  ? 2
                               : 1;
  } else if constexpr (sizeof(T) == 4) {
    return x >= (T(1) << 28)   ? 5
           : x >= (T(1) << 21) ? 4
           : x >= (T(1) << 14) ? 3
           : x >= (T(1) << 7)  ? 2
                               : 1;
  } else if constexpr (sizeof(T) == 2) {
    return x >= (T(1) << 14) ? 3 : x >= (T(1) << 7) ? 2 : 1;
  } else if constexpr (sizeof(T) == 1) {
    return x >= (T(1) << 7) ? 2 : 1;
  } else {
    static_assert(!std::is_same_v<T, T>, "unsupported integer type");
  }
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
template <std::unsigned_integral T>
size_t encode(T x, void* sink) {
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
template <std::unsigned_integral T>
size_t decode(T& x, const void* source) {
  auto in = reinterpret_cast<const uint8_t*>(source);
  size_t i = 0;
  uint8_t low7;
  x = 0;
  do {
    low7 = *in++;
    x |= static_cast<T>(low7 & 0x7f) << (7 * i);
    ++i;
  } while (low7 & 0x80);
  return i;
}

} // namespace tenzir::detail::varbyte
