//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/logical_operator.hpp"

#include <caf/error.hpp>

namespace vast {

/// A type-erased, logical representation of a pipeline consisting of a sequence
/// of logical operators with matching input and output element types.
class logical_pipeline final : public runtime_logical_operator {
public:
  /// Default-constructs an empty logical pipeline.
  logical_pipeline() noexcept = default;

  /// Parses a logical pipeline from its textual representation.
  static auto parse(std::string_view repr) -> caf::expected<logical_pipeline>;

  /// Creates a logical pipeline from a set of logical operators. Flattenes
  /// nested pipelines to ensure that none of the operators are themselves a
  /// pipeline.
  static auto make(std::vector<logical_operator_ptr> ops)
    -> caf::expected<logical_pipeline>;

  /// Returns the input element type of the logical pipeline's first operator,
  /// or the *void* element type if the logical pipeline is empty.
  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type override;

  /// Returns the output element type of the logical pipeline's last operator,
  /// or the *void* element type if the logical pipeline is empty.
  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type override;

  /// Returns whether the pipeline is closed, i.e., both the input and output
  /// element types are *void*.
  [[nodiscard]] auto closed() const noexcept -> bool;

  /// See *runtime_logical_operator::make_runtime_physical_operatorinput_schema,
  /// ctrl)*
  ///
  /// NOTE: A logical pipeline has no single corresponding physical operator.
  /// Calling this function always returns a logic error. To run a pipeline,
  /// create an executor instead, e.g., by calling *make_local_executor()*.
  [[nodiscard]] auto
  make_runtime_physical_operator(const type& input_schema,
                                 operator_control_plane& ctrl) noexcept
    -> caf::expected<runtime_physical_operator> override;

  /// Returns a textual representation of the logical pipeline.
  [[nodiscard]] auto to_string() const noexcept -> std::string override;

  /// Unwraps the logical pipeline, converting it into its logical operators.
  [[nodiscard]] auto unwrap() && -> std::vector<logical_operator_ptr>;

  [[nodiscard]] auto predicate_pushdown(expression const&) const noexcept
    -> std::optional<std::pair<expression, logical_operator_ptr>> override;

  /// Same as `predicate_pushdown`, but returns a `logical_pipeline`.
  [[nodiscard]] auto
  predicate_pushdown_pipeline(expression const&) const noexcept
    -> std::optional<std::pair<expression, logical_pipeline>>;

  /// Returns a trivially-true expression. This is a workaround for having no
  /// empty conjunction. It can also be used in a comparison to detect that an
  /// expression is trivially-true.
  static auto trivially_true_expression() -> const expression& {
    static auto expr = expression{
      predicate{
        meta_extractor{meta_extractor::kind::type},
        relational_operator::not_equal,
        data{std::string{"this expression matches everything"}},
      },
    };
    return expr;
  }

private:
  explicit logical_pipeline(std::vector<logical_operator_ptr> ops)
    : ops_(std::move(ops)) {
  }

  std::vector<logical_operator_ptr> ops_ = {};
};

/// Creates a local executor from the current logical pipeline that allows for
/// running the pipeline in the current thread incrementally.
/// @pre `pipeline.closed()`
auto make_local_executor(logical_pipeline pipeline) noexcept
  -> generator<caf::expected<void>>;

} // namespace vast

template <>
struct fmt::formatter<vast::logical_pipeline> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::logical_pipeline& value, FormatContext& ctx) const {
    auto str = value.to_string();
    return std::copy(str.begin(), str.end(), ctx.out());
  }
};
