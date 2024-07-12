//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file comes from a 3rd party and has been adapted to fit into the Tenzir
// code base. Details about the original file:
//
// - Repository: https://github.com/Microsoft/GSL
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       GSL/include/gsl/gsl_util
// - Author:     Microsoft
// - Copyright:  (c) 2015 Microsoft Corporation. All rights reserved.
// - License:    MIT

#pragma once

#include "tenzir/detail/assert.hpp"

#include <source_location>
#include <type_traits>
#include <utility>

namespace tenzir::detail {

/// A searchable way to do narrowing casts of values.
template <class T, class U>
constexpr T narrow_cast(U&& u) noexcept {
  return static_cast<T>(std::forward<U>(u));
}

template <class T, class U>
struct is_same_signedness
  : public std::bool_constant<std::is_signed<T>::value
                              == std::is_signed<U>::value> {};

/// A checked cast that panics if the cast changed the value.
template <class T, class U>
auto narrow(U y, std::source_location source = std::source_location::current())
  -> T {
  T x = narrow_cast<T>(y);
  if (static_cast<U>(x) != y
      && (is_same_signedness<T, U>::value || (x < T{}) != (y < U{}))) {
    detail::panic_impl(
      fmt::format("cannot narrow {} to {}", y, typeid(T).name()), source);
  }
  return x;
}

} // namespace tenzir::detail
