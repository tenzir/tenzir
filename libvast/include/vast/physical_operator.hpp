//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/element_type.hpp"

namespace vast {

template <element_type Input, element_type Output>
struct physical_operator_traits {
  using type = std::function<auto(generator<element_type_to_batch_t<Input>>)
                               ->generator<element_type_to_batch_t<Output>>>;
};

template <element_type Output>
struct physical_operator_traits<void, Output> {
  using type
    = std::function<auto()->generator<element_type_to_batch_t<Output>>>;
};

/// A physical operator for a given input and output element type. The signature
/// of this function is defined by the *physical_operator_traits*.
///
/// TODO: This should probably be a unique function instead to guarantee pointer
/// stability. We tried using fu2's unique function, but that does not handle
/// copy construction correctly apparently, and CAF's unique function is not
/// implicitly constructible from a lambda which makes for a bad API.
template <element_type Input, element_type Output>
struct physical_operator : physical_operator_traits<Input, Output>::type {
  using super = typename physical_operator_traits<Input, Output>::type;
  using super::super;
};

/// A type-erased version of a physical operator.
///
/// This variant of all physical operators is created dynamically from the list
/// of element types. This code generates all possible combinations
/// *physical_operator<T, U>* for all possible combinations of registered
/// element types. The signature of the physical operator is defined in
/// *physical_operator_traits<T, U>*.
using runtime_physical_operator = caf::detail::tl_apply_t<
  decltype([]<int... Indices>(std::integer_sequence<int, Indices...>) {
    return caf::detail::type_list<physical_operator<
      caf::detail::tl_at_t<
        element_types, Indices / caf::detail::tl_size<element_types>::value>,
      caf::detail::tl_at_t<
        element_types,
        Indices % caf::detail::tl_size<element_types>::value>>...>{};
  }(std::make_integer_sequence<
    int, caf::detail::tl_size<element_types>::value
           * caf::detail::tl_size<element_types>::value>())),
  std::variant>;

} // namespace vast
