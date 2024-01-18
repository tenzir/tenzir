//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/type_traits.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>

namespace tenzir {

/// @relates fnv
enum class fnv_variant { v1, v1a };

/// The Fowler-Noll-Vo hash function.
/// @tparam Bits The size of the ouput; either 32 or 64
/// @tparam Variant The FNV variant that selects the concrete implementation.
template <size_t Bits, fnv_variant Variant>
class fnv {
  static_assert(Bits == 32 || Bits == 64);

  static constexpr auto make_result_type() {
    if constexpr (Bits == 64)
      return uint64_t{};
    if constexpr (Bits == 32)
      return uint32_t{};
  }

public:
  using result_type = decltype(make_result_type());

  static constexpr std::endian endian = std::endian::little;

  // See http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-param for the
  // parameterization
  static constexpr auto prime() -> result_type {
    if constexpr (Bits == 64)
      return 1099511628211ull;
    if constexpr (Bits == 32)
      return 16777619u;
  }

  static constexpr auto offset_basis() -> result_type {
    if constexpr (Bits == 64)
      return 14695981039346656037ull;
    if constexpr (Bits == 32)
      return 2166136261u;
  }

  auto operator()(const void* x, size_t n) noexcept -> void {
    const auto* ptr = static_cast<const unsigned char*>(x);
    for (const auto* end = ptr + n; ptr < end; ++ptr) {
      if constexpr (Variant == fnv_variant::v1) {
        state_ *= prime();
        state_ ^= *ptr;
      } else if constexpr (Variant == fnv_variant::v1a) {
        state_ ^= *ptr;
        state_ *= prime();
      } else {
        static_assert(detail::always_false_v<decltype(Variant)>,
                      "missing implementation");
      }
    }
  }

  explicit operator result_type() noexcept {
    return state_;
  }

private:
  result_type state_ = offset_basis();
};

template <size_t Bits>
using fnv1 = fnv<Bits, fnv_variant::v1>;

template <size_t Bits>
using fnv1a = fnv<Bits, fnv_variant::v1a>;

} // namespace tenzir
