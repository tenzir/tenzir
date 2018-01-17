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

#ifndef VAST_DETAIL_CODING_HPP
#define VAST_DETAIL_CODING_HPP

#include <limits>
#include <type_traits>

namespace vast {
namespace detail {

/// Converts a byte value into an ASCII character.
/// @param b The byte to convert.
/// @returns The ASCII representation of *b*.
/// @relates byte_to_hex hex_to_byte
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char byte_to_char(T b) {
  return b < 10 ? '0' + b : 'a' + b - 10;
}

/// Converts a byte value into a hex value.
/// @param b The byte to convert.
/// @returns b The two hex nibbles as `(high, low)` pair.
/// @relates byte_to_char hex_to_byte
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
std::pair<char, char> byte_to_hex(T b) {
  static constexpr char hex[] = "0123456789ABCDEF";
  return {hex[(b >> 4) & 0x0f], hex[b & 0x0f]};
}

/// Converts a single hex character into its byte value.
/// @param hex The hex character.
/// @returns The byte value of *hex* or 0 if *hex* is not a valid hex char.
/// @relates byte_to_hex byte_to_char
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char hex_to_byte(T hex) {
  if (hex >= '0' && hex <= '9')
    return hex - '0';
  if (hex >= 'A' && hex <= 'F')
    return hex - 'A' + 10;
  if (hex >= 'a' && hex <= 'f')
    return hex - 'a' + 10;
  return '\0';
}

/// Converts two characters representing a hex byte into a single byte value.
/// @param hi The high hex nibble.
/// @param lo The low hex nibble.
/// @relates byte_to_hex byte_to_char
template <
  typename T,
  typename = std::enable_if_t<std::is_integral<T>::value>
>
char hex_to_byte(T hi, T lo) {
  auto byte = hex_to_byte(hi) << 4;
  byte |= hex_to_byte(lo);
  return byte;
}

} // namespace detail
} // namespace vast

#endif
