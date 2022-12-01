//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/execution/logical_operator.hpp"

#include <fmt/format.h>

namespace vast::execution {

/// A pipeline is an ordered sequence of logical operators with matching input
/// and output types along the sequence. The pipeline's input type is the input
/// type of the first operator in the sequence, and the pipeline's output type
/// is the output type of the last operator in the sequence. A pipeline is by
/// definition a logical operator Instantiating a pipeline directly is forbidden.
class pipeline final : public detail::logical_operator_base {
public:
  /// A pipeline itself isn't all that actionable until it is turned into a
  /// plan. That, however, requires giving the plan access to the pipeline's
  /// internals.
  friend class plan;

  /// Create a pipeline from an ordered sequence of logical operators.
  /// @param logical_operators The logical operators sequence to create the
  /// pipeline from.
  /// @pre Adjacent operators form a valid sequence; see @ref check_sequence.
  static auto make(std::vector<logical_operator_ptr> logical_operators)
    -> caf::expected<std::unique_ptr<pipeline>> {
    for (auto&& op : std::exchange(logical_operators, {})) {
      if (!logical_operators.empty()) {
        if (auto err = check_sequence(logical_operators.back(), op))
          return err;
      }
      if (auto* nested_pipeline = dynamic_cast<pipeline*>(op.get())) {
        logical_operators.insert(
          logical_operators.end(),
          std::make_move_iterator(nested_pipeline->logical_operators_.begin()),
          std::make_move_iterator(nested_pipeline->logical_operators_.end()));
      } else {
        logical_operators.push_back(std::move(op));
      }
    }
    return std::make_unique<pipeline>(pipeline{std::move(logical_operators)});
  }

  /// Returns the pipeline's input type, i.e., it's first operator's input type.
  /// @note An empty pipeline's input type is always void.
  [[nodiscard]] auto input_type() const noexcept
    -> detail::runtime_element_type final {
    if (!logical_operators_.empty())
      return logical_operators_.front()->input_type();
    // An empty pipeline's input type is always void.
    using traits = detail::element_type_traits<void>;
    return traits{};
  }

  /// Returns the pipeline's output type, i.e., it's last operator's output
  /// type.
  /// @note An empty pipeline's output type is always void.
  [[nodiscard]] auto output_type() const noexcept
    -> detail::runtime_element_type final {
    if (!logical_operators_.empty())
      return logical_operators_.back()->output_type();
    using traits = detail::element_type_traits<void>;
    return traits{};
  }

  /// Convert a pipeline back into its textural representation.
  [[nodiscard]] auto to_string() const noexcept -> std::string final {
    return fmt::format("{}", fmt::join(logical_operators_, " | "));
  }

private:
  /// Instantiate a physical operator from a pipeline directly is forbidden.
  /// This has to be implemented to allow for pipelines to be used as a logical
  /// operator, but the function always returns a logic error. To instantiate a
  /// pipeline, create a @ref plan from it and pass that to an executor.
  [[nodiscard]] auto make_any(type input_schema)
    -> caf::expected<detail::any_physical_operator> final {
    VAST_ASSERT_CHEAP(input_type().requires_schema
                      == static_cast<bool>(input_schema));
    return caf::make_error(ec::logic_error, "cannot instantiate a pipeline "
                                            "directly");
  }

  /// Returns whether two operators are valid when sequenced.
  /// @param lhs The operator whose output is to be checked.
  /// @param rhs The operator whose input is to be checked.
  static auto check_sequence(const logical_operator_ptr& lhs,
                             const logical_operator_ptr& rhs) noexcept
    -> caf::error {
    if (lhs->output_type().id != rhs->input_type().id)
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("operator sequence '{} | {}' is invalid: output type '{}' "
                    "does not match input type '{}'",
                    lhs->to_string(), rhs->to_string(), lhs->output_type().name,
                    rhs->input_type().name));
    if (lhs->output_type().id == detail::element_type_traits<void>::id
        && rhs->input_type().id == detail::element_type_traits<void>::id)
      return caf::make_error(ec::invalid_argument,
                             fmt::format("operator sequence '{} | {}' is "
                                         "invalid: cannot connect over type "
                                         "'{}'",
                                         lhs->to_string(), rhs->to_string(),
                                         lhs->input_type().name));
    return {};
  }

  /// Construct a pipeline. This private constructor assumes that the operators
  /// form a valid sequence, and that no operator is a pipeline itself.
  explicit pipeline(std::vector<logical_operator_ptr> logical_operators) noexcept
    : logical_operators_{std::move(logical_operators)} {
    // nop
  }

  /// The ordered sequence of pipeline operators.
  std::vector<logical_operator_ptr> logical_operators_ = {};
};

} // namespace vast::execution

namespace fmt {
template <>
struct formatter<std::unique_ptr<vast::execution::pipeline>> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const std::unique_ptr<vast::execution::pipeline>& op,
              FormatContext& ctx) const {
    VAST_ASSERT(op);
    const auto str = op->to_string();
    return std::copy(str.begin(), str.end(), ctx.out());
  }
};

} // namespace fmt
