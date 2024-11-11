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
#include <caf/sum_type_access.hpp>
#include <caf/sum_type_token.hpp>

#include <tuple>
#include <utility>

namespace tenzir::detail {

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

} // namespace tenzir::detail

namespace caf {

template <class T, class Forward>
struct sum_type_access<tenzir::detail::passthrough_type<T, Forward>> final {
  using type0 = T;
  using types = detail::type_list<T>;
  static constexpr bool specialized = true;

  template <int Index>
  static bool
  is(tenzir::detail::passthrough_type<T, Forward>, sum_type_token<T, Index>) {
    return true;
  }

  template <int Index>
  static Forward get(tenzir::detail::passthrough_type<T, Forward> x,
                     sum_type_token<T, Index>) {
    return static_cast<Forward>(x.ref);
  }

  template <class Result, class Visitor, class... Args>
  static Result apply(tenzir::detail::passthrough_type<T, Forward> x,
                      Visitor&& v, Args&&... xs) {
    auto xs_as_tuple = std::forward_as_tuple(xs...);
    auto indices = detail::get_indices(xs_as_tuple);
    return detail::apply_args_suffxied(
      std::forward<Visitor>(v), std::move(indices), xs_as_tuple,
      get(std::move(x), sum_type_token<type0, 0>{}));
  }
};

} // namespace caf

namespace tenzir {
template <class T, class Forward>
  requires has_variant_traits<std::remove_cvref_t<Forward>>
class variant_traits<detail::passthrough_type<T, Forward>> {
  using V = detail::passthrough_type<T, Forward>;
  using backing_traits = variant_traits<std::remove_cvref_t<Forward>>;

public:
  static constexpr auto count = backing_traits::count;

  static auto index(const V& x) -> size_t {
    return backing_traits::index(x.ref);
  }

  template <size_t I>
  static auto get(const V& x) -> decltype(auto) {
    return backing_traits::template get<I>(x.ref);
  }
};

template <class T, class Forward>
  requires(not has_variant_traits<std::remove_cvref_t<Forward>>)
class variant_traits<detail::passthrough_type<T, Forward>> {
  using V = detail::passthrough_type<T, Forward>;

public:
  static constexpr auto count = size_t{1};

  static auto index(const V&) -> size_t {
    return 0;
  }

  template <size_t I>
    requires(I == 0)
  static auto get(const V& x) -> decltype(auto) {
    return x.ref;
  }
};
} // namespace tenzir
