//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/generator.hpp"
#include "vast/execution/detail/element_type.hpp"

#include <caf/detail/type_list.hpp>
#include <caf/expected.hpp>

#include <functional>

namespace vast::execution::detail {

using vast::detail::generator;

/// A *physical operator* is instantiated from a *logical operator* and an
/// (optional) input schema, and maps an input element generator to an output
/// element generator.
/// @tparam Input The input type from the list of @ref supported_element_types.
/// @tparam Output The output type from the list of @ref supported_element_types.
template <element_type Input, element_type Output>
struct physical_operator final
  : std::function<
      auto(generator<caf::expected<Input>>)->generator<caf::expected<Output>>> {
  using super = std::function<
    auto(generator<caf::expected<Input>>)->generator<caf::expected<Output>>>;
  using super::super;
};

// This template black-magic automatically creates a list of all possible
// combinations of element types as supported physical operators. This list's
// size is always the square of the size of the supported element type list's
// size.
//
// If the supported element types were A and B, this would generate this list:
//
//   caf::detail::type_list<
//     physical_operator<A, A>,
//     physical_operator<B, A>,
//     physical_operator<A, B>,
//     physical_operator<B, B>>
//
// This may look a bit overengineered at first, but it makes it so the only
// place that needs to be touched for adding an element type is the list of
// supported element types.
//
// clang-format off
using supported_physical_operator_list = decltype(
  []<int... CombinedIndices>(std::integer_sequence<int, CombinedIndices...>)
      -> caf::detail::type_list<
           physical_operator<
             caf::detail::tl_at_t<supported_element_types,
               CombinedIndices % caf::detail::tl_size<supported_element_types>::value>,
             caf::detail::tl_at_t<supported_element_types,
               CombinedIndices / caf::detail::tl_size< supported_element_types>::value>>
            ...> {
    return {};
  }(std::make_integer_sequence<int,
    caf::detail::tl_size<supported_element_types>::value 
    * caf::detail::tl_size<supported_element_types>::value>()));
// clang-format on

/// A variant of all supported physical operators, generated dynamically from
/// the list of @ref supported_element_types.
using any_physical_operator
  = caf::detail::tl_apply_t<supported_physical_operator_list, std::variant>;

} // namespace vast::execution::detail
