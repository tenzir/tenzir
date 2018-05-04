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

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/Microsoft/GSL
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       GSL/include/gsl/gsl_byte
// - Author:     Microsoft
// - Copyright:  (c) 2015 Microsoft Corporation. All rights reserved.
// - License:    MIT

#pragma once

#include <type_traits>

namespace vast::detail {

// This is a simple definition for now that allows
// use of byte within span<> to be standards-compliant
enum class __attribute__((__may_alias__)) byte : unsigned char {};

template <class IntegerType,
          class = std::enable_if_t<std::is_integral_v<IntegerType>>>
constexpr byte& operator<<=(byte& b, IntegerType shift) noexcept {
  return b = byte(static_cast<unsigned char>(b) << shift);
}

template <class IntegerType,
          class = std::enable_if_t<std::is_integral_v<IntegerType>>>
constexpr byte operator<<(byte b, IntegerType shift) noexcept {
  return byte(static_cast<unsigned char>(b) << shift);
}

template <class IntegerType,
          class = std::enable_if_t<std::is_integral_v<IntegerType>>>
constexpr byte& operator>>=(byte& b, IntegerType shift) noexcept {
  return b = byte(static_cast<unsigned char>(b) >> shift);
}

template <class IntegerType,
          class = std::enable_if_t<std::is_integral_v<IntegerType>>>
constexpr byte operator>>(byte b, IntegerType shift) noexcept {
  return byte(static_cast<unsigned char>(b) >> shift);
}

constexpr byte& operator|=(byte& l, byte r) noexcept {
  return l
         = byte(static_cast<unsigned char>(l) | static_cast<unsigned char>(r));
}

constexpr byte operator|(byte l, byte r) noexcept {
  return byte(static_cast<unsigned char>(l) | static_cast<unsigned char>(r));
}

constexpr byte& operator&=(byte& l, byte r) noexcept {
  return l
         = byte(static_cast<unsigned char>(l) & static_cast<unsigned char>(r));
}

constexpr byte operator&(byte l, byte r) noexcept {
  return byte(static_cast<unsigned char>(l) & static_cast<unsigned char>(r));
}

constexpr byte& operator^=(byte& l, byte r) noexcept {
  return l
         = byte(static_cast<unsigned char>(l) ^ static_cast<unsigned char>(r));
}

constexpr byte operator^(byte l, byte r) noexcept {
  return byte(static_cast<unsigned char>(l) ^ static_cast<unsigned char>(r));
}

constexpr byte operator~(byte b) noexcept {
  return byte(~static_cast<unsigned char>(b));
}

template <class IntegerType,
          class = std::enable_if_t<std::is_integral_v<IntegerType>>>
constexpr IntegerType to_integer(byte b) noexcept {
  return static_cast<IntegerType>(b);
}

template <bool E, typename T>
constexpr byte to_byte_impl(T t) noexcept {
  static_assert(E,
                "to_byte(t) must be provided an unsigned char, otherwise "
                "data loss may occur. "
                "If you are calling to_byte with an integer contant use: "
                "to_byte<t>() version.");
  return static_cast<byte>(t);
}

template <>
constexpr byte to_byte_impl<true, unsigned char>(unsigned char t) noexcept {
  return byte(t);
}

template <typename T>
constexpr byte to_byte(T t) noexcept {
  return to_byte_impl<std::is_same_v<T, unsigned char>, T>(t);
}

template <int I>
constexpr byte to_byte() noexcept {
  static_assert(
    I >= 0 && I <= 255,
    "byte only has 8 bits of storage, values must be in range 0-255");
  return static_cast<byte>(I);
}

} // namespace vast::detail
