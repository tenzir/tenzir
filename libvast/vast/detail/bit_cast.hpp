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

#pragma once

#include <memory>
#include <type_traits>

namespace vast::detail {

/// @param src A trivially copyable type.
/// @returns A value of type To obtained by reinterpreting the object
/// representation of from. Every bit in the value representation of the
/// returned To object is equal to the corresponding bit in the object
/// representation of from. The values of padding bits in the returned To object
/// are unspecified.
/// TODO: Remove this when we have C++20, which ships with a compiler magic
///       version of this with constexpr support.
template <
  typename To, typename From,
  typename = std::enable_if_t<
    (sizeof(To) == sizeof(From))
    && std::conjunction_v<std::is_trivially_copyable<From>, std::is_trivial<To>>>>
To bit_cast(const From& src) noexcept {
  To dst;
  std::memcpy(&dst, &src, sizeof(To));
  return dst;
}

} // namespace vast::detail
