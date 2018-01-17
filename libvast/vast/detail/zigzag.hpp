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

#ifndef VAST_DETAIL_ZIGZAG_HPP
#define VAST_DETAIL_ZIGZAG_HPP

#include <limits>
#include <type_traits>

namespace vast {
namespace detail {

/// The *zig-zag* coding of signed integers into unsigned integers, with the
/// goal to produce small absolute values. The coding works as follows:
///
///      0 => 0
///     -1 => 1
///      1 => 2
///     -2 => 3
///      2 -> 4
///      ...
namespace zigzag {

/// Encodes a signed integral value.
/// @param x The value to encode.
/// @returns The zig-zag-encoded value of *x*.
template <class T>
auto encode(T x)
-> std::enable_if_t<
  std::is_integral<T>::value && std::is_signed<T>{},
  std::make_unsigned_t<T>
> {
  static constexpr auto width = std::numeric_limits<T>::digits;
  return (std::make_unsigned_t<T>(x) << 1) ^ (x >> width);
}

/// Decodes an unsigned integral value.
/// @param x A zig-zag-encoded value.
/// @returns The zig-zag-decoded value of *x*.
template <class T>
auto decode(T x)
-> std::enable_if_t<
  std::is_integral<T>::value && std::is_unsigned<T>{},
  std::make_signed_t<T>
> {
  return (x >> 1) ^ -static_cast<std::make_signed_t<T>>(x & 1);
}

} // namespace zigzag
} // namespace detail
} // namespace vast

#endif
