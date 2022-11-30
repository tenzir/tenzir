//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/assert.hpp"
#include "vast/execution/detail/logical_operator_base.hpp"

#include <fmt/core.h>

namespace vast::execution {

/// A type-erased @ref logical_operator with unique ownership.
using logical_operator_ptr = std::unique_ptr<detail::logical_operator_base>;

/// A logical operator has a known input and output @ref element_type, and is
/// able to instantiate a physical operator with an interface depending on
/// the input and output type for a given input schemna.
/// @tparam Input The input element type.
/// @tparam Output The output element type.
template <detail::element_type Input, detail::element_type Output>
class logical_operator : public detail::logical_operator_mixin<Input, Output> {
public:
  /// This alias makes it easy to refer to the correct physical operator type
  /// within the context of its logical operator.
  using physical_operator = detail::physical_operator<Input, Output>;

  /// The make function is the only function users need to implement when
  /// writing a new logical operator. It is responsible for creating the
  /// physical operator that is itself responsible for the underlying
  /// operation.
  using detail::logical_operator_mixin<Input, Output>::make;

private:
  // -- implementation details ------------------------------------------------

  [[nodiscard]] auto make_any(type input_schema)
    -> caf::expected<detail::any_physical_operator> final {
    if constexpr (detail::element_type_traits<Input>::requires_schema) {
      VAST_ASSERT_CHEAP(input_schema);
      auto result = make(std::move(input_schema));
      if (!result)
        return std::move(result.error());
      return std::move(*result);
    } else {
      VAST_ASSERT_CHEAP(!input_schema);
      auto result = make();
      if (!result)
        return std::move(result.error());
      return std::move(*result);
    }
  }

  [[nodiscard]] auto input_type() const noexcept
    -> detail::runtime_element_type final {
    using traits = detail::element_type_traits<Input>;
    return traits{};
  }

  [[nodiscard]] auto output_type() const noexcept
    -> detail::runtime_element_type final {
    using traits = detail::element_type_traits<Output>;
    return traits{};
  }
};

} // namespace vast::execution

namespace fmt {

template <>
struct formatter<vast::execution::logical_operator_ptr> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::execution::logical_operator_ptr& op,
              FormatContext& ctx) const {
    VAST_ASSERT(op);
    const auto str = op->to_string();
    return std::copy(str.begin(), str.end(), ctx.out());
  }
};

} // namespace fmt
