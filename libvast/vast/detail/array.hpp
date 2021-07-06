//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <array>
#include <type_traits>

namespace vast::detail {

// -- Library Fundamentals TS v2 ----------------------------------------------

namespace make_array_details {

template <class>
struct is_ref_wrapper : std::false_type {};

template <class T>
struct is_ref_wrapper<std::reference_wrapper<T>> : std::true_type {};

template <class T>
using not_ref_wrapper = std::negation<is_ref_wrapper<std::decay_t<T>>>;

template <class D, class...>
struct return_type_helper {
  using type = D;
};

template <class... Types>
struct return_type_helper<void, Types...> : std::common_type<Types...> {
  static_assert(std::conjunction_v<not_ref_wrapper<Types>...>,
                "Types cannot contain reference_wrappers when D is void");
};

template <class D, class... Types>
using return_type
  = std::array<typename return_type_helper<D, Types...>::type, sizeof...(Types)>;

} // namespace make_array_details

/// Creates a `std::array` whose size is equal to the number of arguments and
/// whose elements are initialized from the corresponding arguments.
template <class D = void, class... Types>
constexpr make_array_details::return_type<D, Types...>
make_array(Types&&... t) {
  return {std::forward<Types>(t)...};
}

} // namespace vast::detail
