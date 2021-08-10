//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
template <typename To, typename From>
  requires(sizeof(To) == sizeof(From)
           && std::is_trivially_copyable_v<From> && std::is_trivial_v<To>)
To bit_cast(const From& src)
noexcept {
  To dst;
  std::memcpy(&dst, &src, sizeof(To));
  return dst;
}

} // namespace vast::detail
