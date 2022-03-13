//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/detail/apply_args.hpp>
#include <caf/detail/int_list.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/sum_type_access.hpp>
#include <caf/sum_type_token.hpp>

#include <utility>

namespace vast::detail {

/// @copydoc passthrough
template <class T>
struct passthrough_type {
  const T& ref;
};

/// Binds the reference in a container that can be passed to `caf::visit` to
/// disable visitation for an argument.
template <class T>
auto passthrough(const T& value) noexcept -> passthrough_type<T> {
  return {.ref = value};
}

} // namespace vast::detail

namespace caf {

template <class T>
struct sum_type_access<vast::detail::passthrough_type<T>> final {
  using type0 = T;
  using types = detail::type_list<T>;
  static constexpr bool specialized = true;

  template <int Index>
  static bool
  is(const vast::detail::passthrough_type<T>&, sum_type_token<T, Index>) {
    return true;
  }

  template <int Index>
  static const T&
  get(const vast::detail::passthrough_type<T>& x, sum_type_token<T, Index>) {
    return x.ref;
  }

  template <int Index>
  static const T*
  get_if(const vast::detail::passthrough_type<T>* x, sum_type_token<T, Index>) {
    return &x->ref;
  }

  template <class Result, class Visitor, class... Args>
  static auto
  apply(const vast::detail::passthrough_type<T>& x, Visitor&& v, Args&&... xs)
    -> Result {
    auto xs_as_tuple = std::forward_as_tuple(xs...);
    auto indices = detail::get_indices(xs_as_tuple);
    return detail::apply_args_suffxied(std::forward<Visitor>(v),
                                       std::move(indices), xs_as_tuple,
                                       get(x, sum_type_token<type0, 0>{}));
  }
};

} // namespace caf
