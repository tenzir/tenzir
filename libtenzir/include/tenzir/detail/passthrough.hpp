//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once
#include <tenzir/variant_traits.hpp>

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>

#include <tuple>
#include <utility>

namespace tenzir {
namespace detail {

/// @copydoc passthrough
template <class T, class Forward>
struct passthrough_type {
  [[no_unique_address]] Forward ref;
};

/// Binds the reference in a container that can be passed to `caf::visit` to
/// disable visitation for an argument.
template <class T>
auto passthrough(T&& value) noexcept
  -> passthrough_type<std::remove_cvref_t<T>, T&&> {
  return {.ref = std::forward<T>(value)};
}

} // namespace detail

template <class T, class Forward>
class variant_traits<detail::passthrough_type<T, Forward>> {
  using V = detail::passthrough_type<T, Forward>;

public:
  static constexpr auto count = size_t{1};

  static auto index(const V&) -> size_t {
    return 0;
  }

  template <size_t I>
  static auto get(const V& x) -> decltype(auto) {
    static_assert(I == 0, "passthrough should only ever be visited on index 0");
    return x.ref;
  }
};
} // namespace tenzir
