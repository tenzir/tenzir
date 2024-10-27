//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/type_traits.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <type_traits>

namespace tenzir::policy {

struct uppercase {};
struct lowercase {};

} // namespace tenzir::policy

namespace tenzir::detail {

/// Converts a byte value into an ASCII character.
/// @param b The byte to convert.
/// @returns The ASCII representation of *b*.
/// @relates byte_to_hex hex_to_byte
template <class T>
constexpr auto byte_to_char(T b) -> char {
  static_assert(std::is_integral_v<T> || sizeof(T) == 1);
  auto c = static_cast<char>(b);
  return c < 10 ? '0' + c : 'a' + c - 10;
}

/// Converts a byte value into a hex value with a given alphabet.
/// @param x The byte to convert.
/// @param xs The alphabet to use for conversion (including NUL byte).
/// @returns The two hex nibbles as `(high, low)` pair.
/// @relates byte_to_char hex_to_byte
template <class T>
constexpr auto byte_to_hex(T x, const char (&xs)[16 + 1])
  -> std::pair<char, char> {
  static_assert(std::is_integral_v<T> || sizeof(T) == 1);
  auto hi = static_cast<size_t>((x >> 4) & T{0x0f});
  auto lo = static_cast<size_t>(x & T{0x0f});
  return {xs[hi], xs[lo]};
}

/// Converts a byte value into a hex value.
/// @param x The byte to convert.
/// @returns The two hex nibbles as `(high, low)` pair.
/// @relates byte_to_char hex_to_byte
template <class Policy = policy::uppercase, class T>
constexpr auto byte_to_hex(T x) -> std::pair<char, char> {
  if constexpr (std::is_same_v<Policy, policy::uppercase>) {
    return byte_to_hex(x, "0123456789ABCDEF");
  } else if constexpr (std::is_same_v<Policy, policy::lowercase>) {
    return byte_to_hex(x, "0123456789abcdef");
  } else {
    static_assert(always_false_v<Policy>, "unsupported policy");
  }
}

/// Converts a byte range into a hex string.
/// @param xs The byte sequence to convert into a hex string.
/// @param result The string to append to.
/// @relates byte_to_hex
template <class Policy = policy::lowercase>
void hexify(std::span<const std::byte> xs, std::string& result) {
  for (auto x : xs) {
    auto [hi, lo] = byte_to_hex<Policy>(x);
    result += hi;
    result += lo;
  }
}

/// Converts a byte range into a hex string.
/// @param xs The byte sequence to convert into a hex string.
/// @returns The hex string of *xs*.
/// @relates byte_to_hex
template <class Policy = policy::lowercase>
auto hexify(std::span<const std::byte> xs) -> std::string {
  std::string result;
  hexify<Policy>(xs, result);
  return result;
}

/// Converts a single hex character into its byte value.
/// @param hex The hex character.
/// @returns The byte value of *hex* or 0 if *hex* is not a valid hex char.
/// @relates byte_to_hex byte_to_char
template <class T>
constexpr auto hex_to_byte(T hex) -> char {
  static_assert(std::is_integral_v<T> || sizeof(T) == 1);
  if (hex >= '0' && hex <= '9') {
    return hex - '0';
  }
  if (hex >= 'A' && hex <= 'F') {
    return hex - 'A' + 10;
  }
  if (hex >= 'a' && hex <= 'f') {
    return hex - 'a' + 10;
  }
  return '\0';
}

/// Converts two characters representing a hex byte into a single byte value.
/// @param hi The high hex nibble.
/// @param lo The low hex nibble.
/// @relates byte_to_hex byte_to_char
template <class T>
constexpr auto hex_to_byte(T hi, T lo) -> char {
  static_assert(std::is_integral_v<T> || sizeof(T) == 1);
  return (hex_to_byte(hi) << 4) | hex_to_byte(lo);
}

} // namespace tenzir::detail
